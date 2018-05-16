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
package com.streamsets.datacollector.http;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.MainStandalonePipelineManagerModule;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.task.Task;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.testing.NetworkUtils;
import dagger.ObjectGraph;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class TestStatsREST {

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  private String baseDir;
  private Task server;

  @Before
  public void setup() throws Exception {
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
  }

  @After
  public void cleanup() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
  }

  private String startServer() throws Exception {
    try {

      int port = NetworkUtils.getRandomPort();
      Configuration conf = new Configuration();
      conf.set(WebServerTask.HTTP_PORT_KEY, port);
      conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
      Writer writer = new FileWriter(new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX +
          RuntimeInfo.CONFIG_DIR), "sdc.properties"));
      conf.save(writer);
      writer.close();
      ObjectGraph dagger = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
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
  public void testStatsGET() throws Exception {
    String statsUrl = startServer() + "/rest/v1/system/stats";
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(statsUrl).openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertTrue(conn.getContentType().startsWith("application/json"));
      Map map = ObjectMapperFactory.get().readValue(conn.getInputStream(), Map.class);
      Assert.assertEquals(2, map.size());
      Assert.assertFalse((Boolean) map.get("opted"));
      Assert.assertFalse((Boolean) map.get("active"));
    } finally {
      stopServer();
    }
  }

  @Test
  public void testStatsPOST() throws Exception {
    String statsUrl = startServer() + "/rest/v1/system/stats?active=true";
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(statsUrl).openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty(SSOConstants.X_REST_CALL.toLowerCase(), "test");
      conn.setDoInput(true);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertTrue(conn.getContentType().startsWith("application/json"));
      Map map = ObjectMapperFactory.get().readValue(conn.getInputStream(), Map.class);
      Assert.assertEquals(3, map.size());
      Assert.assertTrue((Boolean) map.get("opted"));
      Assert.assertTrue((Boolean) map.get("active"));
      Assert.assertTrue(map.containsKey("stats"));
    } finally {
      stopServer();
    }
  }

}
