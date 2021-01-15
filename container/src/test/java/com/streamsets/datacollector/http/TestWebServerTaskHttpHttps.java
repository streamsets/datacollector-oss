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
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.activation.NopActivation;
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.FileUserGroupManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.security.usermgnt.TrxUsersManager;
import com.streamsets.datacollector.security.usermgnt.UsersManager;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RegistrationResponseDelegate;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.lib.security.http.SSOService;
import com.streamsets.testing.NetworkUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.streamsets.datacollector.util.AwaitConditionUtil.waitForStart;

public class TestWebServerTaskHttpHttps {

  private RuntimeInfo runtimeInfo;

  static class PingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write("ping");
    }
  }

  @SuppressWarnings("unchecked")
  private WebServerTask createWebServerTask(
      final String confDir,
      final Configuration conf,
      final Set<WebAppProvider> webAppProviders,
      boolean isDPMEnabled
  ) throws Exception {
    runtimeInfo = new StandaloneRuntimeInfo(
        RuntimeInfo.SDC_PRODUCT,
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Collections.emptyList()
    ) {
      @Override
      public String getConfigDir() {
        return confDir;
      }

      @Override
      public String getId() {
        return "sdcId";
      }

      @Override
      public String getAppAuthToken() {
        return "appAuthToken";
      }
    };
    File file = new File("target", UUID.randomUUID().toString());
    try (Writer writer = new FileWriter(file)) {
    }
    UsersManager usersManager = new TrxUsersManager(file);
    runtimeInfo.setDPMEnabled(isDPMEnabled);
    Set<ContextConfigurator> configurators = new HashSet<>();
    configurators.add(new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.addServlet(new ServletHolder(new PingServlet()), "/ping");
        context.addServlet(new ServletHolder(new PingServlet()), "/rest/v1/ping");
        context.addServlet(new ServletHolder(new PingServlet()), "/public-rest/v1/ping");
      }
    });
    return new DataCollectorWebServerTask(
        ProductBuildInfo.getDefault(),
        runtimeInfo,
        conf,
        new NopActivation(),
        configurators,
        webAppProviders,
        new FileUserGroupManager(usersManager),
        null
    );
  }

  @SuppressWarnings("unchecked")
  private WebServerTask createWebServerTask(final String confDir, final Configuration conf) throws Exception {
    return createWebServerTask(confDir, conf, Collections.<WebAppProvider>emptySet(), false);
  }

  private String createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    return dir.getAbsolutePath();
  }

  @Test
  public void testInvalidPorts() throws Exception {
    Configuration conf = new Configuration();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, 0);
    conf.set(WebServerTask.HTTPS_PORT_KEY, 0);
    WebServerTask ws = createWebServerTask(createTestDir(), conf);
    try {
      ws.initTask();
    } catch (IllegalArgumentException iae) {
      //
    }
    conf.set(WebServerTask.HTTP_PORT_KEY, 0);
    conf.set(WebServerTask.HTTPS_PORT_KEY, 10000);
    ws = createWebServerTask(createTestDir(), conf);
    try {
      ws.initTask();
    } catch (IllegalArgumentException iae) {
      //
    }

    conf.set(WebServerTask.HTTP_PORT_KEY, 10000);
    conf.set(WebServerTask.HTTPS_PORT_KEY, 0);
    try {
      ws.initTask();
    } catch (IllegalArgumentException iae) {
      //
    }
  }

  @Test
  public void testGetServerURI() throws Exception {
    Configuration conf = new Configuration();
    int httpPort = NetworkUtils.getRandomPort();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, httpPort);
    final WebServerTask ws = createWebServerTask(createTestDir(), conf);
    ws.initTask();
    // Server hasn't yet started
    try {
      ws.getServerURI();
      Assert.fail("Expected ServerNotYetRunningException but didn't get any");
    } catch (ServerNotYetRunningException se) {
      // Expected
    } catch (Exception e) {
      Assert.fail("Expected ServerNotYetRunningException but got " + e);
    }
    // Now start the server
    try {
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);
      Assert.assertEquals(httpPort, ws.getServerURI().getPort());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testHttp() throws Exception {
    Configuration conf = new Configuration();
    int httpPort = NetworkUtils.getRandomPort();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, httpPort);
    final WebServerTask ws = createWebServerTask(createTestDir(), conf);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);

      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort  + "/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testHttpRandomPort() throws Exception {
    Configuration conf = new Configuration();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, 0);
    final WebServerTask ws = createWebServerTask(createTestDir(), conf);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);

      HttpURLConnection conn =
          (HttpURLConnection) new URL("http://127.0.0.1:" + ws.getServerURI().getPort() + "/ping")
              .openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } finally {
      ws.stopTask();
    }
  }

  //making the url connection to trust a self signed cert on localhost
  static void configureHttpsUrlConnection(HttpsURLConnection conn) throws Exception {
    SSLContext sc = SSLContext.getInstance("SSL");
    TrustManager[] trustAllCerts = new TrustManager[] {
        new X509TrustManager() {
          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }
          @Override
          public void checkClientTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }
          @Override
          public void checkServerTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }
        }
    };
    sc.init(null, trustAllCerts, new java.security.SecureRandom());
    conn.setSSLSocketFactory(sc.getSocketFactory());
    conn.setHostnameVerifier((s, sslSession) -> true);
  }

  @Test
  public void testHttps() throws Exception {
    testHttpsWithKeyStore("sdc-keystore.jks");
  }

  @Test
  public void testHttpsUsingKeystoreFileWithMultipleCerts() throws Exception {
    testHttpsWithKeyStore("sdc-keystore-with-multiple-certs.jks");
  }

  public void testHttpsWithKeyStore(String keystoreFileName) throws Exception {
    Configuration conf = new Configuration();
    int httpsPort = NetworkUtils.getRandomPort();
    String confDir = createTestDir();
    String keyStore = new File(confDir, keystoreFileName).getAbsolutePath();
    OutputStream os = new FileOutputStream(keyStore);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream(keystoreFileName),os);
    os.close();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, -1);
    conf.set(WebServerTask.HTTPS_PORT_KEY, httpsPort);
    conf.set(WebServerTask.HTTPS_KEYSTORE_PATH_KEY, keystoreFileName);
    conf.set(WebServerTask.HTTPS_KEYSTORE_PASSWORD_KEY, "password");
    conf.set(WebServerTask.HTTPS_TRUSTSTORE_PATH_KEY, "");
    final WebServerTask ws = createWebServerTask(confDir, conf);
    try {
      ws.initTask();
      new Thread(ws::runTask).start();
      waitForStart(ws);
      HttpsURLConnection conn = (HttpsURLConnection) new URL("https://127.0.0.1:" + httpsPort + "/ping")
          .openConnection();
      configureHttpsUrlConnection(conn);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNotNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testHttpsRandomPort() throws Exception {
    Configuration conf = new Configuration();
    String confDir = createTestDir();
    String keyStore = new File(confDir, "sdc-keystore.jks").getAbsolutePath();
    OutputStream os = new FileOutputStream(keyStore);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("sdc-keystore.jks"), os);
    os.close();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, -1);
    conf.set(WebServerTask.HTTPS_PORT_KEY, 0);
    conf.set(WebServerTask.HTTPS_KEYSTORE_PATH_KEY, "sdc-keystore.jks");
    conf.set(WebServerTask.HTTPS_KEYSTORE_PASSWORD_KEY, "password");
    final WebServerTask ws = createWebServerTask(confDir, conf);
    try {
      ws.initTask();
      new Thread(ws::runTask).start();

      waitForStart(ws);

      HttpsURLConnection conn =
          (HttpsURLConnection) new URL("https://127.0.0.1:" + ws.getServerURI().getPort() + "/ping")
              .openConnection();
      configureHttpsUrlConnection(conn);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertNotNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testHttpRedirectToHttps() throws Exception {
    Configuration conf = new Configuration();
    int httpPort = NetworkUtils.getRandomPort();
    int httpsPort = NetworkUtils.getRandomPort();
    String confDir = createTestDir();
    String keyStore = new File(confDir, "sdc-keystore.jks").getAbsolutePath();
    OutputStream os = new FileOutputStream(keyStore);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("sdc-keystore.jks"),os);
    os.close();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, httpPort);
    conf.set(WebServerTask.HTTPS_PORT_KEY, httpsPort);
    conf.set(WebServerTask.HTTPS_KEYSTORE_PATH_KEY, "sdc-keystore.jks");
    conf.set(WebServerTask.HTTPS_KEYSTORE_PASSWORD_KEY, "password");
    final WebServerTask ws = createWebServerTask(confDir, conf);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);
      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort + "/ping").openConnection();
      conn.setInstanceFollowRedirects(false);
      Assert.assertTrue(conn.getResponseCode() >= 300 && conn.getResponseCode() < 400);
      Assert.assertTrue(conn.getHeaderField("Location").startsWith("https://"));
      HttpsURLConnection conns = (HttpsURLConnection) new URL(conn.getHeaderField("Location")).openConnection();
      configureHttpsUrlConnection(conns);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conns.getResponseCode());
      Assert.assertNotNull(runtimeInfo.getSSLContext());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testWebApp() throws Exception {
    WebAppProvider webAppProvider = new WebAppProvider() {
      @Override
      public ServletContextHandler get() {
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/webapp");
        handler.addServlet(new ServletHolder(new PingServlet()), "/ping");
        return handler;
      }

      @Override
      public Configuration getAppConfiguration() {
        return new Configuration();
      }

      @Override
      public void postStart() {
      }
    };
    Configuration conf = new Configuration();
    int httpPort = NetworkUtils.getRandomPort();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, httpPort);
    final WebServerTask ws = createWebServerTask(createTestDir(), conf, ImmutableSet.of(webAppProvider), false);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);

      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort + "/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      conn = (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort + "/webapp/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } finally {
      ws.stopTask();
    }
  }

  private static class DummySSOService implements SSOService {
    boolean delegated;

    @Override
    public void setDelegateTo(SSOService ssoService) {

    }

    @Override
    public SSOService getDelegateTo() {
      return null;
    }

    @Override
    public void setConfiguration(Configuration configuration) {
    }

    @Override
    public void register(Map<String, String> attributes) {
    }

    @Override
    public String createRedirectToLoginUrl(
        String requestUrl,
        boolean duplicateRedirect,
        HttpServletRequest req,
        HttpServletResponse res
    ) {
      return null;
    }

    @Override
    public String getLoginPageUrl() {
      return null;
    }

    @Override
    public String getLogoutUrl() {
      return null;
    }


    @Override
    public SSOPrincipal validateUserToken(String authToken) {
      return null;
    }

    @Override
    public boolean invalidateUserToken(String authToken) {
      return false;
    }


    @Override
    public SSOPrincipal validateAppToken(
        String authToken, String componentId
    ) {
      return null;
    }

    @Override
    public boolean invalidateAppToken(String authToken) {
      return false;
    }

    @Override
    public void clearCaches() {

    }

    @Override
    public void setRegistrationResponseDelegate(RegistrationResponseDelegate delegate) {
    }
  }

  @Test
  public void testWebAppSSOServiceDelegation() throws Exception {
    final DummySSOService delegatedTo = new DummySSOService();
    final Configuration conf = new Configuration();
    conf.set(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "authToken");
    WebAppProvider webAppProvider = new WebAppProvider() {
      @Override
      public ServletContextHandler get() {
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/webapp");
        handler.addEventListener(new ServletContextListener() {
          @Override
          public void contextInitialized(ServletContextEvent sce) {
            SSOService ssoService = (SSOService) sce.getServletContext().getAttribute(SSOService.SSO_SERVICE_KEY);
            ssoService.setDelegateTo(delegatedTo);
            delegatedTo.delegated = true;
          }

          @Override
          public void contextDestroyed(ServletContextEvent sce) {

          }
        });
        handler.addServlet(new ServletHolder(new PingServlet()), "/ping");
        return handler;
      }

      @Override
      public Configuration getAppConfiguration() {
        return conf;
      }

      @Override
      public void postStart() {
      }
    };
    int httpPort = NetworkUtils.getRandomPort();
    conf.set(WebServerTask.HTTP_PORT_KEY, httpPort);
    final WebServerTask ws = createWebServerTask(createTestDir(), conf, ImmutableSet.of(webAppProvider), true);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          try {
            ws.runTask();
          } catch (Exception ex) {
            // Ignore Registration failure exception
          }

        }
      }.start();
      waitForStart(ws);

      Assert.assertTrue(delegatedTo.delegated);
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testAuthorizationConstraints() throws Exception {
    WebAppProvider webAppProvider = new WebAppProvider() {
      @Override
      public ServletContextHandler get() {
        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/webapp");
        handler.addServlet(new ServletHolder(new PingServlet()), "/ping");
        handler.addServlet(new ServletHolder(new PingServlet()), "/rest/v1/ping");
        handler.addServlet(new ServletHolder(new PingServlet()), "/public-rest/v1/ping");
        return handler;
      }

      @Override
      public Configuration getAppConfiguration() {
        return new Configuration();
      }

      @Override
      public void postStart() {
      }
    };
    Configuration conf = new Configuration();
    int httpPort = NetworkUtils.getRandomPort();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "basic");
    conf.set(WebServerTask.HTTP_PORT_KEY, httpPort);
    String confDir = createTestDir();
    File realmFile = new File(confDir, "basic-realm.properties");
    try (
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("basic-realm.properties");
        OutputStream os = new FileOutputStream(realmFile)
    ) {
      IOUtils.copy(is, os);
    }
    Set<PosixFilePermission> set = new HashSet<>();
    set.add(PosixFilePermission.OWNER_EXECUTE);
    set.add(PosixFilePermission.OWNER_READ);
    set.add(PosixFilePermission.OWNER_WRITE);
    Files.setPosixFilePermissions(realmFile.toPath(), set);

    final WebServerTask ws = createWebServerTask(confDir, conf, ImmutableSet.of(webAppProvider), false);
    try {
      ws.initTask();
      new Thread() {
        @Override
        public void run() {
          ws.runTask();
        }
      }.start();
      waitForStart(ws);

      String baseUrl = "http://127.0.0.1:" + httpPort;

      // root app
      HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/ping").openConnection();
      conn.setRequestProperty(CsrfProtectionFilter.HEADER_NAME, "CSRF");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      conn = (HttpURLConnection) openWithBasicAuth(new URL(baseUrl + "/ping"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      conn = (HttpURLConnection) new URL(baseUrl + "/rest/v1/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
      conn = (HttpURLConnection) openWithBasicAuth(new URL(baseUrl + "/rest/v1/ping"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      conn = (HttpURLConnection) new URL(baseUrl + "/public-rest/v1/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      conn = (HttpURLConnection) openWithBasicAuth(new URL(baseUrl + "/public-rest/v1/ping"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // web app
      conn = (HttpURLConnection) new URL(baseUrl + "/webapp/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      conn = (HttpURLConnection) openWithBasicAuth(new URL(baseUrl + "/webapp/ping"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      conn = (HttpURLConnection) new URL(baseUrl + "/webapp/rest/v1/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
      conn = (HttpURLConnection) openWithBasicAuth(new URL(baseUrl + "/webapp/rest/v1/ping"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      conn = (HttpURLConnection) new URL(baseUrl + "/webapp/public-rest/v1/ping").openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      conn = (HttpURLConnection) openWithBasicAuth(new URL(baseUrl + "/webapp/public-rest/v1/ping"));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testKeystoreAbsolutePath() {
    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTPS_KEYSTORE_PATH_KEY, "/tmp/absolute/sdc-keystore.jks");
    File keystore = WebServerTask.getHttpsKeystore(conf, "/tmp/config");
    String absolutePath = keystore.getAbsolutePath();

    Assert.assertEquals("/tmp/absolute/sdc-keystore.jks", absolutePath);
  }

  @Test
  public void testKeystoreRelativePath() {
    Configuration conf = new Configuration();
    conf.set(WebServerTask.HTTPS_KEYSTORE_PATH_KEY, "sdc-keystore.jks");
    File keystore = WebServerTask.getHttpsKeystore(conf, "/tmp/config");
    String absolutePath = keystore.getAbsolutePath();

    Assert.assertEquals("/tmp/config/sdc-keystore.jks", absolutePath);
  }

  private URLConnection openWithBasicAuth(URL url) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    byte[] authEncBytes = Base64.encodeBase64("admin:admin".getBytes());
    String authStringEnc = new String(authEncBytes);
    conn.setRequestProperty("Authorization", "Basic " + authStringEnc);
    return conn;
  }
}
