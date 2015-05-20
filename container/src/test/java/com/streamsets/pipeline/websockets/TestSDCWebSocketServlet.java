/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.websockets;

import com.streamsets.pipeline.config.MetricElement;
import com.streamsets.pipeline.config.MetricType;
import com.streamsets.pipeline.config.MetricsRuleDefinition;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.log.LogUtils;
import com.streamsets.pipeline.main.PipelineTask;
import com.streamsets.pipeline.main.PipelineTaskModule;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.prodmanager.StandalonePipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import com.streamsets.pipeline.util.Configuration;
import dagger.ObjectGraph;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestSDCWebSocketServlet {

  private static String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  private static int getRandomPort() throws Exception {
    ServerSocket ss = new ServerSocket(0);
    int port = ss.getLocalPort();
    ss.close();
    return port;
  }

  private static String baseDir;
  private static Task server;
  private static String baseURL;
  private static StandalonePipelineManagerTask pipelineManager;

  @BeforeClass
  public static void setup() throws Exception {
    server = null;
    baseDir = createTestDir();
    Assert.assertTrue(new File(baseDir, "etc").mkdir());
    Assert.assertTrue(new File(baseDir, "data").mkdir());
    Assert.assertTrue(new File(baseDir, "log").mkdir());
    Assert.assertTrue(new File(baseDir, "web").mkdir());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, baseDir + "/etc");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, baseDir + "/data");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, baseDir + "/log");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, baseDir + "/web");
    baseURL = startServer();
  }

  @AfterClass
  public static void cleanup() {
    stopServer();
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
  }

  private static String startServer() throws  Exception {
    int port = getRandomPort();

    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    Writer writer = writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
      RuntimeInfo.CONFIG_DIR), "sdc.properties"));
    conf.save(writer);
    writer.close();

    File logFile = new File(baseDir + "/log", "sdc.log");
    writer = new FileWriter(logFile);
    writer.write("hello\n");
    writer.close();

    File log4fConfig = new File(baseDir, "log4j.properties");
    writer = new FileWriter(log4fConfig);
    writer.write(LogUtils.LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY + "=" + logFile.getAbsolutePath());
    writer.close();


    ObjectGraph dagger = ObjectGraph.create(PipelineTaskModule.class);


    RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
    runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR, new URL("file://" + baseDir + "/log4j.properties"));

    server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();

    TaskWrapper task = dagger.get(TaskWrapper.class);
    PipelineTask pipelineTask = (PipelineTask) ((TaskWrapper)task).getTask();
    pipelineManager = (StandalonePipelineManagerTask)pipelineTask.getProductionPipelineManagerTask();
    return "ws://127.0.0.1:" + port;
  }

  private static void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testStatusWebSocket() throws Exception {
    String statusWSURI = baseURL + "/rest/v1/webSocket?type=status";
    WebSocketClient client = new WebSocketClient();
    try {

      SimpleEchoSocket socket = new SimpleEchoSocket();
      client.start();
      URI echoUri = new URI(statusWSURI);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      client.connect(socket, echoUri, request);
      System.out.printf("Connecting to : %s%n", echoUri);

      Thread.sleep(500);

      //Changing state of pipeline triggers the Status WebSocket to send message
      pipelineManager.setState("myPipeline", "1.0", State.STOPPED, "Pipeline Stopped", null);

      socket.awaitClose(2, TimeUnit.SECONDS);

      Assert.assertEquals(true, socket.isConnected());
      Assert.assertEquals(1, socket.getMessages().size());

    } finally {
      client.stop();
    }
  }

  @Test
  public void testAlertsWebSocket() throws Exception {
    String statusWSURI = baseURL + "/rest/v1/webSocket?type=alerts";
    WebSocketClient client = new WebSocketClient();
    try {

      SimpleEchoSocket socket = new SimpleEchoSocket();
      client.start();
      URI echoUri = new URI(statusWSURI);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      client.connect(socket, echoUri, request);
      System.out.printf("Connecting to : %s%n", echoUri);

      Thread.sleep(500);

      //Trigger Alert
      MetricsRuleDefinition metricsRuleDefinition = new MetricsRuleDefinition("testCounterMatch", "testCounterMatch",
        "testCounterMatch", MetricType.COUNTER, MetricElement.COUNTER_COUNT, "${value()>98}", false, true);
      pipelineManager.broadcastAlerts(metricsRuleDefinition);

      metricsRuleDefinition = new MetricsRuleDefinition("testCounterMatch", "testCounterMatch",
        "testCounterMatch", MetricType.COUNTER, MetricElement.COUNTER_COUNT, "${value()>98}", false, true);
      pipelineManager.broadcastAlerts(metricsRuleDefinition);

      socket.awaitClose(1, TimeUnit.SECONDS);
      Assert.assertEquals(true, socket.isConnected());
      Assert.assertEquals(2, socket.getMessages().size());

    } finally {
      client.stop();
    }
  }

  @Test
  public void testMetricsWebSocket() throws Exception {
    String statusWSURI = baseURL + "/rest/v1/webSocket?type=metrics";
    WebSocketClient client = new WebSocketClient();
    try {

      SimpleEchoSocket socket = new SimpleEchoSocket();
      client.start();
      URI echoUri = new URI(statusWSURI);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      client.connect(socket, echoUri, request);
      System.out.printf("Connecting to : %s%n", echoUri);

      Thread.sleep(500);

      //Set state to RUNNING so that Metrics WebSocket will send message
      pipelineManager.setState("myPipeline", "1.0", State.RUNNING, "Pipeline Running", null);

      socket.awaitClose(3, TimeUnit.SECONDS);

      Assert.assertEquals(true, socket.isConnected());
      Assert.assertTrue(socket.getMessages().size() > 0);

      pipelineManager.setState("myPipeline", "1.0", State.STOPPED, "Pipeline Stopped", null);

    } finally {
      client.stop();
    }
  }

  @Test
  public void testLogMessageWebSocket() throws Exception {
    String statusWSURI = baseURL + "/rest/v1/webSocket?type=log";
    WebSocketClient client = new WebSocketClient();
    try {

      SimpleEchoSocket socket = new SimpleEchoSocket();
      client.start();
      URI echoUri = new URI(statusWSURI);
      ClientUpgradeRequest request = new ClientUpgradeRequest();
      client.connect(socket, echoUri, request);
      System.out.printf("Connecting to : %s%n", echoUri);
      Thread.sleep(500);

      socket.awaitClose(1, TimeUnit.SECONDS);

      Assert.assertEquals(true, socket.isConnected());
    } finally {
      client.stop();
    }
  }
}
