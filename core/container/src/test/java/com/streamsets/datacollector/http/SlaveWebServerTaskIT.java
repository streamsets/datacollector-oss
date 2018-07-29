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

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.activation.NopActivation;
import com.streamsets.datacollector.http.TestWebServerTaskHttpHttps.PingServlet;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.FileUserGroupManager;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
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

import static com.streamsets.datacollector.util.AwaitConditionUtil.waitForStart;

public class SlaveWebServerTaskIT {

  private SlaveRuntimeInfo runtimeInfo;

  private WebServerTask createSlaveWebServerTask(final String confDir, final Configuration conf, final
  Set<WebAppProvider> webAppProviders) throws Exception {
    runtimeInfo = new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), Collections
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
    return new SlaveWebServerTask(
        new DataCollectorBuildInfo(),
        runtimeInfo,
        conf,
        new NopActivation(),
        configurators,
        webAppProviders,
        new FileUserGroupManager()
    );
  }

  private WebServerTask createSlaveWebServerTask(final String confDir, final Configuration conf) throws Exception {
    return createSlaveWebServerTask(confDir, conf, Collections.<WebAppProvider>emptySet());
  }

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  @Before
  public void setup() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
  }

  @After
  public void tearDown() {
    System.clearProperty("sdc.testing-mode");
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
    verifyWebServerTask(ws, runtimeInfo);
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
    verifyWebServerTask(ws, runtimeInfo);
  }

  private static void verifyWebServerTask(final WebServerTask ws, SlaveRuntimeInfo runtimeInfo) throws Exception {
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);
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
      waitForStart(ws);
      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + ws.getServerURI().getPort() +
          "/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

}
