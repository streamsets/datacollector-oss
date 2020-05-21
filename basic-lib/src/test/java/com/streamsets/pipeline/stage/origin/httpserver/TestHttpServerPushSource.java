/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.lib.http.AbstractHttpReceiverServer;
import com.streamsets.pipeline.lib.http.HttpConstants;
import com.streamsets.pipeline.lib.http.HttpReceiverServer;
import com.streamsets.pipeline.lib.httpsource.HttpSourceConfigs;
import com.streamsets.pipeline.lib.tls.CredentialValueBean;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.testing.NetworkUtils;
import org.apache.commons.io.IOUtils;
import org.awaitility.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.awaitility.Awaitility.await;

public class TestHttpServerPushSource {

  @Test
  public void testSource() throws Exception {
    HttpSourceConfigs httpConfigs = new HttpSourceConfigs();
    httpConfigs.appIds = new ArrayList<>();
    httpConfigs.appIds.add(new CredentialValueBean("id"));
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;
    httpConfigs.tlsConfigBean.tlsEnabled = false;
    HttpServerPushSource source =
        new HttpServerPushSource(httpConfigs, 1, DataFormat.TEXT, new DataParserFormatConfig());
    final PushSourceRunner runner =
        new PushSourceRunner.Builder(HttpServerDPushSource.class, source).addOutputLane("a").build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
        }
      });

      // wait for the HTTP server up and running
      HttpReceiverServer httpServer = (HttpReceiverServer)Whitebox.getInternalState(source, "server");
      await().atMost(Duration.TEN_SECONDS).until(isServerRunning(httpServer));

      HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:" + httpConfigs.getPort())
          .openConnection();
      connection.setRequestMethod("POST");
      connection.setUseCaches(false);
      connection.setDoOutput(true);
      connection.setRequestProperty(Constants.X_SDC_APPLICATION_ID_HEADER, "id");
      connection.setRequestProperty("customHeader", "customHeaderValue");
      connection.getOutputStream().write("Hello".getBytes());
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("Hello", records.get(0).get("/text").getValue());
      Assert.assertEquals(
          "id",
          records.get(0).getHeader().getAttribute(Constants.X_SDC_APPLICATION_ID_HEADER)
      );
      Assert.assertEquals("customHeaderValue", records.get(0).getHeader().getAttribute("customHeader"));

      // passing App Id via query param should fail when appIdViaQueryParamAllowed is false
      String url = "http://localhost:" + httpConfigs.getPort() +
          "?" + HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM + "=id";
      Response response = ClientBuilder.newClient()
          .target(url)
          .request()
          .post(Entity.json("Hello"));
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, response.getStatus());

      runner.setStop();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testWithAppIdViaQueryParam() throws Exception {
    HttpSourceConfigs httpConfigs = new HttpSourceConfigs();
    httpConfigs.appIds = new ArrayList<>();
    httpConfigs.appIds.add(new CredentialValueBean("id"));
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;
    httpConfigs.tlsConfigBean.tlsEnabled = false;
    httpConfigs.appIdViaQueryParamAllowed = true;
    HttpServerPushSource source =
        new HttpServerPushSource(httpConfigs, 1, DataFormat.TEXT, new DataParserFormatConfig());
    final PushSourceRunner runner =
        new PushSourceRunner.Builder(HttpServerDPushSource.class, source).addOutputLane("a").build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
        }
      });

      // wait for the HTTP server up and running
      HttpReceiverServer httpServer = (HttpReceiverServer)Whitebox.getInternalState(source, "server");
      await().atMost(Duration.TEN_SECONDS).until(isServerRunning(httpServer));


      String url = "http://localhost:" + httpConfigs.getPort() +
          "?" + HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM + "=id";
      Response response = ClientBuilder.newClient()
          .target(url)
          .request()
          .post(Entity.json("Hello"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
      Assert.assertEquals(1, records.size());
      Assert.assertEquals("Hello", records.get(0).get("/text").getValue());

      // for security reasons, the track http method should be disallowed
      url = "http://localhost:" + httpConfigs.getPort() +
          "?" + HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM + "=id";
      response = ClientBuilder.newClient()
          .target(url)
          .request()
          .trace();
      Assert.assertEquals(HttpURLConnection.HTTP_BAD_METHOD, response.getStatus());

      // Passing wrong App ID in query param should return 403 response
      url = "http://localhost:" + httpConfigs.getPort() +
          "?" + HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM + "=wrongid";
      response = ClientBuilder.newClient()
          .target(url)
          .request()
          .post(Entity.json("Hello"));
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, response.getStatus());

      runner.setStop();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAvroData() throws Exception {
    HttpSourceConfigs httpConfigs = new HttpSourceConfigs();
    httpConfigs.appIds = new ArrayList<>();
    httpConfigs.appIds.add(new CredentialValueBean("id"));
    httpConfigs.port = NetworkUtils.getRandomPort();
    httpConfigs.maxConcurrentRequests = 1;
    httpConfigs.tlsConfigBean.tlsEnabled = false;
    httpConfigs.appIdViaQueryParamAllowed = true;
    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    dataFormatConfig.avroSchemaSource = OriginAvroSchemaSource.SOURCE;
    HttpServerPushSource source =
            new HttpServerPushSource(httpConfigs, 1, DataFormat.AVRO, dataFormatConfig);
    final PushSourceRunner runner =
            new PushSourceRunner.Builder(HttpServerDPushSource.class, source).addOutputLane("a").build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
        }
      });

      // wait for the HTTP server up and running
      HttpReceiverServer httpServer = (HttpReceiverServer)Whitebox.getInternalState(source, "server");
      await().atMost(Duration.TEN_SECONDS).until(isServerRunning(httpServer));

      String url = "http://localhost:" + httpConfigs.getPort() +
              "?" + HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM + "=id";
      File avroDataFile = SdcAvroTestUtil.createAvroDataFile();
      InputStream in = new FileInputStream(avroDataFile);
      byte[] avroData = IOUtils.toByteArray(in);
      Response response = ClientBuilder.newClient()
              .target(url)
              .request()
              .post(Entity.entity(avroData, MediaType.APPLICATION_OCTET_STREAM_TYPE));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
      Assert.assertEquals(3, records.size());
      Assert.assertEquals("a", records.get(0).get("/name").getValue());
      Assert.assertEquals("b", records.get(1).get("/name").getValue());
      Assert.assertEquals("c", records.get(2).get("/name").getValue());
      runner.setStop();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      runner.runDestroy();
    }
  }

  public static Callable<Boolean> isServerRunning(AbstractHttpReceiverServer httpServer ) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return httpServer.isRunning();
      }
    };
  }
}
