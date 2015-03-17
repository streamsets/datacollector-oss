/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.log.LogUtils;
import com.streamsets.pipeline.main.PipelineTaskModule;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import com.streamsets.pipeline.util.Configuration;
import dagger.ObjectGraph;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestLogServlet {

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  private int getRandomPort() throws Exception {
    ServerSocket ss = new ServerSocket(0);
    int port = ss.getLocalPort();
    ss.close();
    return port;
  }

  private String baseDir;
  private Task server;
  private static File logFile;
  private static File oldLogFile;

  @Before
  public void setup() throws Exception {
    server = null;
    baseDir = createTestDir();
    String log4jConf = new File(baseDir, "log4j.properties").getAbsolutePath();
    File logFile = new File(baseDir, "x.log");
    Writer writer = new FileWriter(log4jConf);
    IOUtils.write(LogUtils.LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY + "=" + logFile.getAbsolutePath(), writer);
    writer.close();
    Assert.assertTrue(new File(baseDir, "etc").mkdir());
    Assert.assertTrue(new File(baseDir, "data").mkdir());
    Assert.assertTrue(new File(baseDir, "log").mkdir());
    Assert.assertTrue(new File(baseDir, "web").mkdir());
    System.setProperty(RuntimeInfo.CONFIG_DIR, baseDir + "/etc");
    System.setProperty(RuntimeInfo.DATA_DIR, baseDir + "/data");
    System.setProperty(RuntimeInfo.LOG_DIR, baseDir + "/log");
    System.setProperty(RuntimeInfo.STATIC_WEB_DIR, baseDir + "/web");
  }

  @After
  public void cleanup() {
    System.getProperties().remove(RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeInfo.STATIC_WEB_DIR);
  }

  private String startServer() throws  Exception {
    logFile = new File(baseDir, "test.log");
    Writer writer = new FileWriter(logFile);
    writer.write("hello\n");
    writer.close();
    Thread.sleep(1000); // for log files to have different lastModified timestamp
    oldLogFile = new File(baseDir, "test.log.1");
    writer = new FileWriter(oldLogFile);
    writer.write("bye\n");
    writer.close();
    File log4fConfig = new File(baseDir, "log4j.properties");
    writer = new FileWriter(log4fConfig);
    writer.write(LogUtils.LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY + "=" + logFile.getAbsolutePath());
    writer.close();

    int port = getRandomPort();
    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTP_PORT_KEY, port);
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    writer = new FileWriter(new File(System.getProperty(RuntimeInfo.CONFIG_DIR), "sdc.properties"));
    conf.save(writer);
    writer.close();
    ObjectGraph dagger = ObjectGraph.create(PipelineTaskModule.class);
    RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
    runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR, new URL("file://" + baseDir + "/log4j.properties"));
    server = dagger.get(TaskWrapper.class);
    server.init();
    server.run();
    return "http://127.0.0.1:" + port;
  }

  private void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testLogs() throws Exception {
      String baseLogUrl = startServer() + "/rest/v1/log";
      try {
      HttpURLConnection conn = (HttpURLConnection) new URL(baseLogUrl + "/files").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertTrue(conn.getContentType().startsWith("application/json"));
      List list = ObjectMapperFactory.get().readValue(conn.getInputStream(), List.class);
      Assert.assertEquals(2, list.size());
      for (int i = 0; i < 2; i++) {
        Map map = (Map) list.get(i);
        String log = (String) map.get("file");
        if (log.equals("test.log")) {
          Assert.assertEquals(logFile.lastModified(), (long) map.get("lastModified"));
        } else {
          Assert.assertEquals(oldLogFile.lastModified(), (long) map.get("lastModified"));
        }
      }
      conn = (HttpURLConnection) new URL(baseLogUrl + "/files/" + oldLogFile.getName()).openConnection();
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(conn.getContentType().startsWith("text/plain"));
      List<String> lines = IOUtils.readLines(conn.getInputStream());
      Assert.assertEquals(1, lines.size());
      Assert.assertEquals("bye", lines.get(0));
      } finally {
        stopServer();
      }

  }

}
