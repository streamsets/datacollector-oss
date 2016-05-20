/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.http.TestWebServerTaskHttpHttps.PingServlet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.util.Configuration;

public class SlaveWebServerTaskIT {

  private RuntimeInfo runtimeInfo;

  private WebServerTask createSlaveWebServerTask(final String confDir, final Configuration conf, final
  Set<WebAppProvider> webAppProviders) throws Exception {
    runtimeInfo = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), Collections
        .<ClassLoader>emptyList()) {
      @Override
      public String getConfigDir() {
        return confDir;
      }
    };
    Set<ContextConfigurator> configurators = new HashSet<>();
    configurators.add(new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.addServlet(new ServletHolder(new PingServlet()), "/ping");
        context.addServlet(new ServletHolder(new PingServlet()), "/rest/v1/ping");
        context.addServlet(new ServletHolder(new PingServlet()), "/public-rest/v1/ping");
      }
    });
    return new SlaveWebServerTask(runtimeInfo, conf, configurators, webAppProviders);
  }

  private WebServerTask createSlaveWebServerTask(final String confDir, final Configuration conf) throws Exception {
    return createSlaveWebServerTask(confDir, conf, Collections.<WebAppProvider>emptySet());
  }

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  @Test
  public void testSlaveHttps() throws Exception {
    Configuration conf = new Configuration();
    String confDir = createTestDir();
    String keyStore = new File(confDir, "sdc-keystore.jks").getAbsolutePath();
    OutputStream os = new FileOutputStream(keyStore);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("sdc-keystore.jks"), os);
    os.close();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, -1);
    conf.set(WebServerTask.HTTPS_PORT_KEY, 0);
    conf.set(SlaveWebServerTask.HTTPS_WORKER_KEYSTORE_PATH, keyStore);
    conf.set(SlaveWebServerTask.HTTPS_WORKER_KEYSTORE_PASSWORD, "password");
    conf.set(SlaveWebServerTask.HTTPS_WORKER_TRUSTSTORE_PATH, keyStore);
    conf.set(SlaveWebServerTask.HTTPS_WORKER_TRUSTSTORE_PASSWORD, "password");
    final WebServerTask ws = createSlaveWebServerTask(confDir, conf);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      Thread.sleep(1000);
      HttpsURLConnection conn = (HttpsURLConnection) new URL("https://127.0.0.1:" + ws.getServerURI().getPort() +
          "/ping").openConnection();
      TestWebServerTaskHttpHttps.configureHttpsUrlConnection(conn);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNotNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testSlaveHttpsPasswordAbsolute() throws Exception {
    Configuration conf = new Configuration();
    String confDir = createTestDir();
    String keyStore = new File(confDir, "sdc-keystore.jks").getAbsolutePath();
    OutputStream os = new FileOutputStream(keyStore);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("sdc-keystore.jks"), os);
    os.close();
    try (PrintWriter out = new PrintWriter(new File(confDir, "sdc-keystore-password"))) {
      out.println("password");
    }
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, -1);
    conf.set(WebServerTask.HTTPS_PORT_KEY, 0);
    conf.set(SlaveWebServerTask.HTTPS_WORKER_KEYSTORE_PATH, keyStore);
    conf.set(SlaveWebServerTask.HTTPS_WORKER_KEYSTORE_PASSWORD, "password");
    conf.set(SlaveWebServerTask.HTTPS_WORKER_TRUSTSTORE_PATH, keyStore);
    conf.set(SlaveWebServerTask.HTTPS_WORKER_TRUSTSTORE_PASSWORD, "password");
    final WebServerTask ws = createSlaveWebServerTask(confDir, conf);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      Thread.sleep(1000);
      HttpsURLConnection conn = (HttpsURLConnection) new URL("https://127.0.0.1:" + ws.getServerURI().getPort() +
          "/ping").openConnection();
      TestWebServerTaskHttpHttps.configureHttpsUrlConnection(conn);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNotNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testSlaveHttp() throws Exception {
    Configuration conf = new Configuration();
    String confDir = createTestDir();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, 0);
    conf.set(WebServerTask.HTTPS_PORT_KEY, -1);
    final WebServerTask ws = createSlaveWebServerTask(confDir, conf);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      Thread.sleep(1000);
      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + ws.getServerURI().getPort() +
          "/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

}
