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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.httpsource.RawHttpConfigs;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import com.streamsets.pipeline.stage.origin.httpserver.HttpServerDPushSource;
import com.streamsets.pipeline.stage.origin.httpserver.TestHttpServerPushSource;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.testing.NetworkUtils;
import org.awaitility.Duration;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;

public class TestRestServicePushSource {

  @Test
  public void testRestServiceOrigin() throws Exception {
    RawHttpConfigs httpConfigs = new RawHttpConfigs();
    httpConfigs.appId = () -> "id";
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;
    httpConfigs.tlsConfigBean.tlsEnabled = false;

    RestServiceResponseConfigBean responseConfigBean = new RestServiceResponseConfigBean();
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
          if (record.has("/sendToError")) {
            Map<String, Object> allAttributes = new HashMap<>(record.getHeader().getAllAttributes());
            allAttributes.put("_.errorMessage", "sample error");
            allAttributes.put(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "500");
            record.getHeader().overrideUserAndSystemAttributes(allAttributes);
          } else {
            record.getHeader().setAttribute(RestServiceReceiver.STATUS_CODE_RECORD_HEADER_ATTR_NAME, "200");
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

}
