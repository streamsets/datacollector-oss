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
package com.streamsets.pipeline.stage.origin.websocketserver;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.HttpConstants;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import com.streamsets.testing.NetworkUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestWebSocketServerPushSource {

  @Test
  public void testSource() throws Exception {
    WebSocketConfigs webSocketConfigs = new WebSocketConfigs();
    webSocketConfigs.appId = () -> "appId";
    webSocketConfigs.port = NetworkUtils.getRandomPort();
    webSocketConfigs.maxConcurrentRequests = 1;
    webSocketConfigs.tlsConfigBean.tlsEnabled = false;
    WebSocketServerPushSource source =
        new WebSocketServerPushSource(webSocketConfigs, DataFormat.JSON, new DataParserFormatConfig());
    final PushSourceRunner runner =
        new PushSourceRunner.Builder(WebSocketServerPushSource.class, source).addOutputLane("a").build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });

      WebSocketClient client = new WebSocketClient();
      SimpleEchoSocket socket = new SimpleEchoSocket();
      try {
        client.start();
        URI echoUri = new URI("ws://localhost:" + webSocketConfigs.getPort());
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setHeader(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "appId");
        Future<Session> future = client.connect(socket, echoUri, request);
        future.get();
        // wait for closed socket connection.
        socket.awaitClose(5, TimeUnit.SECONDS);
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        try {
          client.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      runner.waitOnProduce();

      Assert.assertEquals(1, records.size());
      Assert.assertEquals("value", records.get(0).get("/field1").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWithAppIdViaQueryParam() throws Exception {
    WebSocketConfigs webSocketConfigs = new WebSocketConfigs();
    webSocketConfigs.appId = () -> "appId";
    webSocketConfigs.port = NetworkUtils.getRandomPort();
    webSocketConfigs.maxConcurrentRequests = 1;
    webSocketConfigs.tlsConfigBean.tlsEnabled = false;
    webSocketConfigs.appIdViaQueryParamAllowed = true;
    WebSocketServerPushSource source =
        new WebSocketServerPushSource(webSocketConfigs, DataFormat.JSON, new DataParserFormatConfig());
    final PushSourceRunner runner =
        new PushSourceRunner.Builder(WebSocketServerPushSource.class, source).addOutputLane("a").build();
    runner.runInit();
    try {
      final List<Record> records = new ArrayList<>();
      runner.runProduce(Collections.<String, String>emptyMap(), 1, new PushSourceRunner.Callback() {
        @Override
        public void processBatch(StageRunner.Output output) {
          records.clear();
          records.addAll(output.getRecords().get("a"));
          runner.setStop();
        }
      });

      WebSocketClient client = new WebSocketClient();
      SimpleEchoSocket socket = new SimpleEchoSocket();
      try {
        client.start();
        URI echoUri = new URI("ws://localhost:" + webSocketConfigs.getPort() +
            "?" + HttpConstants.SDC_APPLICATION_ID_QUERY_PARAM + "=appId");
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        client.connect(socket, echoUri, request);
        // wait for closed socket connection.
        socket.awaitClose(5, TimeUnit.SECONDS);
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        try {
          client.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      runner.waitOnProduce();

      Assert.assertEquals(1, records.size());
      Assert.assertEquals("value", records.get(0).get("/field1").getValue());
    } finally {
      runner.runDestroy();
    }
  }

}
