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
package com.streamsets.datacollector.execution.alerts;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineWebhookConfig;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.pipeline.api.ExecutionMode;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;

public class TestWebhookNotifier {
  private Server server;
  private static RuntimeInfo runtimeInfo;
  private static boolean serverRequested = false;
  private static String requestPayload = null;

  @Before
  public void setUp() throws Exception {
    int port =  getFreePort();
    server = new Server(InetSocketAddress.createUnresolved("localhost", port));
    server.setHandler(new AbstractHandler() {
      @Override
      public void handle(
          String target,
          Request baseRequest,
          HttpServletRequest request,
          HttpServletResponse response
      ) throws IOException, ServletException {
        serverRequested = true;
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        try {
          BufferedReader reader = request.getReader();
          while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
          }
          requestPayload = stringBuilder.toString();
        } catch (Exception e) { /*report an error*/ }
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
      }
    });
    server.start();
    runtimeInfo = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(TestWebhookNotifier.class.getClassLoader()));
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  @Test
  public void testWebhookNotifierRunError() throws Exception {
    PipelineConfigBean pipelineConfigBean = new PipelineConfigBean();
    pipelineConfigBean.notifyOnStates = ImmutableList.of(com.streamsets.datacollector.config.PipelineState.RUN_ERROR);
    PipelineWebhookConfig webhookConfig = new PipelineWebhookConfig();
    webhookConfig.payload = "{\n  \"text\" : \"Pipeline '{{PIPELINE_TITLE}}' state changed to {{PIPELINE_STATE}} at " +
        "{{TIME}}. \\n <{{PIPELINE_URL}}|Click here> for details!\"\n}";
    webhookConfig.webhookUrl = server.getURI().toString();
    pipelineConfigBean.webhookConfigs = ImmutableList.of(webhookConfig);


    WebHookNotifier webHookNotifier = new WebHookNotifier(
        "x",
        "x",
        "0",
        pipelineConfigBean,
        runtimeInfo,
        null
    );

    PipelineState runningState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUNNING, "Running",
        System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState runErrorState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUN_ERROR, "Run Error",
        System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    webHookNotifier.onStateChange(runningState, runErrorState, "", null, null);

    Assert.assertTrue(serverRequested);
    Assert.assertTrue(requestPayload != null);
    Assert.assertTrue(requestPayload.contains("state changed to RUN_ERROR"));
  }

}
