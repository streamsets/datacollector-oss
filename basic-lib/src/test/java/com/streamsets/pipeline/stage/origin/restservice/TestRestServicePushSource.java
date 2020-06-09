/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.restservice;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import com.streamsets.pipeline.lib.microservice.ResponseConfigBean;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import com.streamsets.pipeline.stage.origin.httpserver.HttpServerDPushSource;
import com.streamsets.pipeline.stage.origin.httpserver.TestHttpServerPushSource;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.pipeline.stage.util.tls.TLSTestUtils;
import com.streamsets.testing.NetworkUtils;
import org.awaitility.Duration;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

public class TestRestServicePushSource {

  @Test
  public void testRestServiceOrigin() {
    HttpSourceConfigs httpConfigs = new HttpSourceConfigs();
    httpConfigs.appIds = ImmutableList.of(new CredentialValueBean("id"));
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;
    httpConfigs.tlsConfigBean.tlsEnabled = false;

    ResponseConfigBean responseConfigBean = new ResponseConfigBean();
    responseConfigBean.dataFormat = DataFormat.JSON;
    responseConfigBean.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    RestServicePushSource source = new RestServicePushSource(
        httpConfigs,
        1,
        DataFormat.JSON,
        new DataParserFormatConfig(),
        responseConfigBean
    );

    final PushSourceRunner runner = new PushSourceRunner
        .Builder(HttpServerDPushSource.class, source)
        .addOutputLane("a")
        .build();
    runner.runInit();

    String httpServerUrl = "http://localhost:" + httpConfigs.getPort();

    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.emptyMap(), 1, output -> {
        List<Record> outputRecords = output.getRecords().get("a");

        records.clear();
        records.addAll(outputRecords);

        runner.getSourceResponseSink().getResponseRecords().clear();
        records.forEach(record -> {
          Record.Header header = record.getHeader();
          if (record.has("/sendToError")) {
            Record.Header spyHeader = Mockito.spy(header);
            Mockito.when(spyHeader.getErrorMessage()).thenReturn("sample error");
            Whitebox.setInternalState(record, "header", spyHeader);
            header.setAttribute(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "500");
          } else {
            header.setAttribute(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "200");
          }

          // Set custom response headers
          header.setAttribute(RestServiceReceiver.RESPONSE_HEADER_ATTR_NAME_PREFIX + "test", "value");

          runner.getSourceResponseSink().addResponse(record);
        });
      });

      // wait for the HTTP server up and running
      RestServiceReceiverServer httpServer = (RestServiceReceiverServer) Whitebox.getInternalState(
          source,
          "server"
      );
      await().atMost(Duration.TEN_SECONDS).until(TestHttpServerPushSource.isServerRunning(httpServer));

      testEmptyPayloadRequest("GET", httpServerUrl, records);
      testEmptyPayloadRequest("HEAD", httpServerUrl, records);
      testEmptyPayloadRequest("DELETE", httpServerUrl, records);
      testEmptyPayloadRequest("POST", httpServerUrl, records);
      testEmptyPayloadRequest("PUT", httpServerUrl, records);
      testEmptyPayloadRequest("PATCH", httpServerUrl, records);

      testPayloadRequest("POST", httpServerUrl, records);
      testPayloadRequest("PUT", httpServerUrl, records);
      testPayloadRequest("PATCH", httpServerUrl, records);

      testErrorResponse(httpServerUrl, records);
      testMultiStatusResponse(httpServerUrl, records);

      runner.setStop();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      runner.runDestroy();
    }
  }

  private void testEmptyPayloadRequest(String method, String httpServerUrl, List<Record> requestRecords) {
    Response response = ClientBuilder.newClient()
        .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
        .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
        .target(httpServerUrl)
        .request()
        .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
        .method(method);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    String responseBody = response.readEntity(String.class);

    Assert.assertEquals(1, requestRecords.size());
    Record.Header emptyPayloadRecordHeader = requestRecords.get(0).getHeader();
    Assert.assertEquals(
        "true",
        emptyPayloadRecordHeader.getAttribute(RestServiceReceiver.EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME)
    );
    Assert.assertEquals(method, emptyPayloadRecordHeader.getAttribute(RestServiceReceiver.METHOD_HEADER));


    // check custom HTTP Response header
    Assert.assertNotNull(response.getHeaders().getFirst("test"));
    Assert.assertEquals("value", response.getHeaders().getFirst("test"));
  }

  private void testPayloadRequest(
      String method,
      String httpServerUrl,
      List<Record> requestRecords
  ) {
    Response response = ClientBuilder.newClient()
        .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
        .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
        .target(httpServerUrl)
        .request()
        .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
        .method(method, Entity.json("{\"f1\": \"abc\", \"f2\": \"xyz\"}"));

    // Test Request Records
    Assert.assertEquals(1, requestRecords.size());
    Record.Header payloadRecord = requestRecords.get(0).getHeader();
    Assert.assertEquals(method, payloadRecord.getAttribute(RestServiceReceiver.METHOD_HEADER));
    Assert.assertNull(payloadRecord.getAttribute(RestServiceReceiver.EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME));
    Assert.assertEquals("abc", requestRecords.get(0).get("/f1").getValue());
    Assert.assertEquals("xyz", requestRecords.get(0).get("/f2").getValue());

    // Test Response from REST Service
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    ResponseEnvelope responseBody = response.readEntity(ResponseEnvelope.class);
    Assert.assertNotNull(responseBody);
    Assert.assertEquals(200, responseBody.getHttpStatusCode());
    Assert.assertNotNull(responseBody.getData());
    Assert.assertEquals(1, responseBody.getData().size());
    Assert.assertNotNull(responseBody.getError());
    Assert.assertEquals(0, responseBody.getError().size());
    Assert.assertNull(responseBody.getErrorMessage());
  }


  private void testErrorResponse(
      String httpServerUrl,
      List<Record> requestRecords
  ) {
    Response response = ClientBuilder.newClient()
        .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
        .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
        .target(httpServerUrl)
        .request()
        .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
        .method(
            "POST",
            Entity.json("{\"f1\": \"abc\", \"f2\": \"xyz\", \"sendToError\": \"Sample Error message\"}")
        );

    // Test Request Records
    Assert.assertEquals(1, requestRecords.size());
    Record.Header payloadRecord = requestRecords.get(0).getHeader();
    Assert.assertEquals("POST", payloadRecord.getAttribute(RestServiceReceiver.METHOD_HEADER));
    Assert.assertNull(payloadRecord.getAttribute(RestServiceReceiver.EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME));
    Assert.assertEquals("abc", requestRecords.get(0).get("/f1").getValue());
    Assert.assertEquals("xyz", requestRecords.get(0).get("/f2").getValue());

    // Test Response from REST Service
    Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getStatus());
    ResponseEnvelope responseBody = response.readEntity(ResponseEnvelope.class);
    Assert.assertNotNull(responseBody);
    Assert.assertEquals(500, responseBody.getHttpStatusCode());
    Assert.assertNotNull(responseBody.getData());
    Assert.assertEquals(0, responseBody.getData().size());
    Assert.assertNotNull(responseBody.getError());
    Assert.assertEquals(1, responseBody.getError().size());
    Assert.assertNotNull(responseBody.getErrorMessage());
  }


  private void testMultiStatusResponse(
      String httpServerUrl,
      List<Record> requestRecords
  ) {
    Response response = ClientBuilder.newClient()
        .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
        .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
        .target(httpServerUrl)
        .request()
        .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
        .method(
            "POST",
            Entity.json("{\"f1\": \"abc\", \"f2\": \"xyz\"}\n{\"f1\": \"abc\", \"f2\": \"xyz\", \"sendToError\": \"Sample Error message\"}")
        );

    // Test Request Records
    Assert.assertEquals(2, requestRecords.size());
    Record.Header payloadRecord = requestRecords.get(0).getHeader();
    Assert.assertEquals("POST", payloadRecord.getAttribute(RestServiceReceiver.METHOD_HEADER));
    Assert.assertNull(payloadRecord.getAttribute(RestServiceReceiver.EMPTY_PAYLOAD_RECORD_HEADER_ATTR_NAME));
    Assert.assertEquals("abc", requestRecords.get(0).get("/f1").getValue());
    Assert.assertEquals("xyz", requestRecords.get(0).get("/f2").getValue());

    // Test Response from REST Service
    Assert.assertEquals(207, response.getStatus());
    ResponseEnvelope responseBody = response.readEntity(ResponseEnvelope.class);
    Assert.assertNotNull(responseBody);
    Assert.assertEquals(207, responseBody.getHttpStatusCode());
    Assert.assertNotNull(responseBody.getData());
    Assert.assertEquals(1, responseBody.getData().size());
    Assert.assertNotNull(responseBody.getError());
    Assert.assertEquals(1, responseBody.getError().size());
    Assert.assertNotNull(responseBody.getErrorMessage());
  }


  //TODO - SDC-12324 causes this test to fail inexplicably. Passes locally, no issues with STF.
  @Ignore
  @Test
  public void testMLTS() throws Exception {
    // Server TLS
    String hostname = TLSTestUtils.getHostname();
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File serverKeyStore = new File(testDir, "serverKeystore.jks");
    Assert.assertTrue(testDir.mkdirs());
    String keyStorePassword = "keystore";
    File serverTrustStore = new File(testDir, "serverTruststore.jks");
    KeyPair keyPair = TLSTestUtils.generateKeyPair();
    Certificate cert = TLSTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
    String trustStorePassword = "truststore";
    TLSTestUtils.createKeyStore(serverKeyStore.toString(), keyStorePassword, "web", keyPair.getPrivate(), cert);
    TLSTestUtils.createTrustStore(serverTrustStore.toString(), trustStorePassword, "web", cert);

    // Client TLS
    File clientKeyStore = new File(testDir, "clientKeystore.jks");
    File clientTrustStore = new File(testDir, "clientTruststore.jks");
    KeyPair clientKeyPair = TLSTestUtils.generateKeyPair();
    Certificate clientCert = TLSTestUtils.generateCertificate("CN=" + hostname, clientKeyPair, 30);
    TLSTestUtils.createKeyStore(clientKeyStore.toString(), keyStorePassword, "web", clientKeyPair.getPrivate(), clientCert);
    TLSTestUtils.createTrustStore(clientTrustStore.toString(), trustStorePassword, "web", clientCert);

    HttpSourceConfigs httpConfigs = new HttpSourceConfigs();
    httpConfigs.appIds = ImmutableList.of(new CredentialValueBean("id"));
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;

    // TLS + MTLS Enabled
    httpConfigs.tlsConfigBean.tlsEnabled = true;
    httpConfigs.needClientAuth = true;

    httpConfigs.tlsConfigBean.keyStoreFilePath = serverKeyStore.getAbsolutePath();
    httpConfigs.tlsConfigBean.keyStorePassword = () -> keyStorePassword;

    httpConfigs.tlsConfigBean.trustStoreFilePath = clientTrustStore.getAbsolutePath();
    httpConfigs.tlsConfigBean.trustStorePassword = () -> trustStorePassword;

    ResponseConfigBean responseConfigBean = new ResponseConfigBean();
    responseConfigBean.dataFormat = DataFormat.JSON;
    responseConfigBean.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    RestServicePushSource source = new RestServicePushSource(
        httpConfigs,
        1,
        DataFormat.JSON,
        new DataParserFormatConfig(),
        responseConfigBean
    );

    final PushSourceRunner runner = new PushSourceRunner
        .Builder(HttpServerDPushSource.class, source)
        .addOutputLane("a")
        .build();
    runner.runInit();

    String httpServerUrl = "https://" + hostname + ":" + httpConfigs.getPort();

    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.emptyMap(), 1, output -> {
        List<Record> outputRecords = output.getRecords().get("a");

        records.clear();
        records.addAll(outputRecords);

        runner.getSourceResponseSink().getResponseRecords().clear();
        records.forEach(record -> {
          Record.Header header = record.getHeader();
          if (record.has("/sendToError")) {
            Record.Header spyHeader = Mockito.spy(header);
            Mockito.when(spyHeader.getErrorMessage()).thenReturn("sample error");
            Whitebox.setInternalState(record, "header", spyHeader);
            header.setAttribute(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "500");
          } else {
            header.setAttribute(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "200");
          }
          runner.getSourceResponseSink().addResponse(record);
        });
      });

      // wait for the HTTP server up and running
      RestServiceReceiverServer httpServer = (RestServiceReceiverServer) Whitebox.getInternalState(
          source,
          "server"
      );
      await().atMost(Duration.TEN_SECONDS).until(TestHttpServerPushSource.isServerRunning(httpServer));

      // Test without passing client certificate


      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(TlsConfigBean.DEFAULT_KEY_MANAGER_ALGORITHM);
      KeyStore keyStore = KeyStore.getInstance("JKS");
      try (final InputStream keyStoreIs = new FileInputStream(clientKeyStore)){
        keyStore.load(keyStoreIs, keyStorePassword.toCharArray());
      }
      keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

      TrustManagerFactory trustStoreFactory = TrustManagerFactory.getInstance(TlsConfigBean.DEFAULT_KEY_MANAGER_ALGORITHM);
      KeyStore trustKeyStore = KeyStore.getInstance("JKS");
      try (final InputStream trustKeyStoreIs = new FileInputStream(serverTrustStore)){
        trustKeyStore.load(trustKeyStoreIs, trustStorePassword.toCharArray());
      }
      trustStoreFactory.init(trustKeyStore);


      // Test Without Client key
      try {
        SSLContext sslContextWithoutClientKey = SSLContext.getInstance("TLS");
        sslContextWithoutClientKey.init(null, trustStoreFactory.getTrustManagers(), null);
        ClientBuilder.newBuilder().sslContext(sslContextWithoutClientKey).build()
            .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .target(httpServerUrl)
            .request()
            .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
            .get();
        Assert.fail();
      } catch (Exception ex) {
        Assert.assertTrue(ex.getMessage().contains("bad_certificate"));
      }

      // Test with client key
      SSLContext sslContextWithClientKey = SSLContext.getInstance("TLS");
      sslContextWithClientKey.init(keyManagerFactory.getKeyManagers(), trustStoreFactory.getTrustManagers(), null);
      Response response = ClientBuilder.newBuilder().sslContext(sslContextWithClientKey).build()
          .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
          .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
          .target(httpServerUrl)
          .request()
          .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
          .get();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());

      runner.setStop();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testSendingRawResponse() throws Exception {
    HttpSourceConfigs httpConfigs = new HttpSourceConfigs();
    httpConfigs.appIds = ImmutableList.of(new CredentialValueBean("id"));
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;
    httpConfigs.tlsConfigBean.tlsEnabled = false;

    ResponseConfigBean responseConfigBean = new ResponseConfigBean();
    responseConfigBean.sendRawResponse = true;
    responseConfigBean.dataFormat = DataFormat.JSON;
    responseConfigBean.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    RestServicePushSource source = new RestServicePushSource(
        httpConfigs,
        1,
        DataFormat.JSON,
        new DataParserFormatConfig(),
        responseConfigBean
    );

    final PushSourceRunner runner = new PushSourceRunner
        .Builder(HttpServerDPushSource.class, source)
        .addOutputLane("a")
        .build();
    runner.runInit();

    String httpServerUrl = "http://localhost:" + httpConfigs.getPort();

    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.emptyMap(), 1, output -> {
        List<Record> outputRecords = output.getRecords().get("a");

        records.clear();
        records.addAll(outputRecords);

        runner.getSourceResponseSink().getResponseRecords().clear();
        records.forEach(record -> {
          Map<String, Field> rootField = new LinkedHashMap<>();
          rootField.put("text", Field.create("It's 80 degrees right now."));

          List<Field> attachmentsField = new ArrayList<>();
          Map<String, Field> attachmentField = new LinkedHashMap<>();
          attachmentField.put("text", Field.create("Partly cloudy today and tomorrow"));
          attachmentsField.add(Field.create(attachmentField));
          rootField.put("attachments", Field.create(attachmentsField));

          record.set(Field.create(rootField));
          Record.Header header = record.getHeader();
          header.setAttribute(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "200");
          runner.getSourceResponseSink().addResponse(record);
        });
      });

      // wait for the HTTP server up and running
      RestServiceReceiverServer httpServer = (RestServiceReceiverServer) Whitebox.getInternalState(
          source,
          "server"
      );
      await().atMost(Duration.TEN_SECONDS).until(TestHttpServerPushSource.isServerRunning(httpServer));


      Response response = ClientBuilder.newClient()
          .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
          .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
          .target(httpServerUrl)
          .request()
          .header(Constants.X_SDC_APPLICATION_ID_HEADER, "id")
          .get();

      // Test Raw Response without envelope from REST Service
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
      Map<String, Object> responseBody = response.readEntity(Map.class);
      Assert.assertNotNull(responseBody);
      Assert.assertTrue(responseBody.containsKey("text"));
      Assert.assertEquals(responseBody.get("text"), "It's 80 degrees right now.");
      Assert.assertTrue(responseBody.containsKey("attachments"));
      runner.setStop();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      runner.runDestroy();
    }
  }

}
