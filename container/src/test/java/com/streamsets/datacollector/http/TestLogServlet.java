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
package com.streamsets.datacollector.http;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.log.LogUtils;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.testing.NetworkUtils;
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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestLogServlet {

  private static final String CONVERSION_PATTERN = "%d{ISO8601} [user:%X{s-user}] [pipeline:%X{s-entity}] [thread:%t] %-5p %c{1} - %m%n";

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
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
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, baseDir + "/etc");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, baseDir + "/data");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, baseDir + "/log");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, baseDir + "/web");
  }

  @After
  public void cleanup() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
  }

  private String startServer() throws  Exception {
    try {
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
      writer.write(LogUtils.LOG4J_APPENDER_STREAMSETS_FILE_PROPERTY + "=" + logFile.getAbsolutePath() + "\n");
      writer.write(LogUtils.LOG4J_APPENDER_STREAMSETS_LAYOUT_CONVERSION_PATTERN + "=" + CONVERSION_PATTERN);
      writer.close();

      int port = NetworkUtils.getRandomPort();
      Configuration conf = new Configuration();
      conf.set(WebServerTask.HTTP_PORT_KEY, port);
      conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
      writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR), "sdc.properties"));
      conf.save(writer);
      writer.close();
      ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
      RuntimeInfo runtimeInfo = dagger.get(RuntimeInfo.class);
      runtimeInfo.setAttribute(RuntimeInfo.LOG4J_CONFIGURATION_URL_ATTR, new URL("file://" + baseDir + "/log4j.properties"));
      server = dagger.get(TaskWrapper.class);

      server.init();
      server.run();
      return "http://127.0.0.1:" + port;
    } catch (Exception e) {
      System.out.println("Got exception " + e);
      return null;
    }
  }

  private void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testLogs() throws Exception {
    String baseLogUrl = startServer() + "/rest/v1/system/logs";
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
