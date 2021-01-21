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
import com.google.common.collect.ImmutableSet;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;

public class TestEmailNotifier {

  private static GreenMail server;
  private static EmailSender emailSender;
  private static RuntimeInfo runtimeInfo;

  @Before
  public void setUp() throws Exception {
    ServerSetup serverSetup = new ServerSetup(getFreePort(), "localhost", "smtp");
    server = new GreenMail(serverSetup);
    server.setUser("user@x", "user", "password");
    server.start();

    Configuration conf = new Configuration();
    conf.set("mail.smtp.host", "localhost");
    conf.set("mail.smtp.port", Integer.toString(server.getSmtp().getPort()));
    emailSender = new EmailSender(conf);

    runtimeInfo = new StandaloneRuntimeInfo(
        RuntimeInfo.SDC_PRODUCT,
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Arrays.asList(TestEmailNotifier.class.getClassLoader())
    );
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
  public void testEmailNotifierRunError() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("x", "x", "0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar"),
      ImmutableSet.of("RUN_ERROR"));

    PipelineState runningState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUNNING, "Running",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState runErrorState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUN_ERROR, "Run Error",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(runningState, runErrorState, "", null, null);

    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers != null);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - x - ERROR"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }

  @Test
  public void testEmailNotifierStartError() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("x", "x","0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar")
      , ImmutableSet.of("START_ERROR"));

    PipelineState startingState = new PipelineStateImpl("x", "x", "0", PipelineStatus.STARTING, "Starting",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState startErrorState = new PipelineStateImpl("x", "x", "0", PipelineStatus.START_ERROR, "Start Error",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(startingState, startErrorState, "", null, null);

    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers != null);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - x - ERROR"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }

  @Test
  public void testEmailNotifierFinished() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("x", "x","0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar"),
      ImmutableSet.of("FINISHED"));

    PipelineState runningState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUNNING, "Running",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState finishedState = new PipelineStateImpl("x", "x", "0", PipelineStatus.FINISHED, "Finished",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(runningState, finishedState, "", null, null);

    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers != null);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - x - FINISHED"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }

  @Test
  public void testEmailNotifierStopped() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("x", "x","0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar"),
      ImmutableSet.of("STOPPED"));

    PipelineState stoppingState = new PipelineStateImpl("x", "x", "0", PipelineStatus.STOPPING, "Stopping",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState stoppedState = new PipelineStateImpl("x", "x", "0", PipelineStatus.STOPPED, "Stopped",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(stoppingState, stoppedState, "", null, null);

    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers != null);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - x - STOPPED"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }

  @Test
  public void testEmailNotifierDisconnected() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("x", "x","0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar"),
      ImmutableSet.of("DISCONNECTED"));

    PipelineState disconnectingState = new PipelineStateImpl("x", "x", "0", PipelineStatus.DISCONNECTING, "Disconnecting",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState disconnectedState = new PipelineStateImpl("x", "x", "0", PipelineStatus.DISCONNECTED, "Disconnected",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(disconnectingState, disconnectedState, "", null, null);

    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers != null);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - x - DISCONNECTED"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }

  @Test
  public void testEmailNotifierConnecting() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("x", "x","0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar"),
      ImmutableSet.of("CONNECTING"));

    PipelineState disconnectedState = new PipelineStateImpl("x", "x", "0", PipelineStatus.DISCONNECTED, "Disconnected",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState connectingState = new PipelineStateImpl("x", "x", "0", PipelineStatus.CONNECTING, "Connecting",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(disconnectedState, connectingState, "", null, null);

    String headers = GreenMailUtil.getHeaders(server.getReceivedMessages()[0]);
    Assert.assertTrue(headers != null);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - x - CONNECTING"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(server.getReceivedMessages()[0]));
  }


  @Test
  public void testEmailNotifierWrongPipeline() throws Exception {

    EmailNotifier emailNotifier = new EmailNotifier("y", "x","0", runtimeInfo, emailSender, ImmutableList.of("foo", "bar"),
      ImmutableSet.of("RUN_ERROR"));

    PipelineState runningState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUNNING, "Running",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    PipelineState runErrorState = new PipelineStateImpl("x", "x", "0", PipelineStatus.RUN_ERROR, "Run Error",
      System.currentTimeMillis(), new HashMap<String, Object>(), ExecutionMode.STANDALONE, "", 0, 0);
    emailNotifier.onStateChange(runningState, runErrorState, "", null, null);

    Assert.assertTrue(server.getReceivedMessages().length == 0);

  }
}
