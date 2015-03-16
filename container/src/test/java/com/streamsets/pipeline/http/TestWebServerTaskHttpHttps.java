/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.http;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.util.Configuration;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TestWebServerTaskHttpHttps {

  private static class PingServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.setStatus(HttpServletResponse.SC_OK);
      resp.getWriter().write("ping");
    }
  }

  @SuppressWarnings("unchecked")
  private WebServerTask createWebServerTask(final String confDir, final Configuration conf) throws Exception {
    RuntimeInfo ri = new RuntimeInfo(Collections.EMPTY_LIST) {
      @Override
      public String getConfigDir() {
        return confDir;
      }
    };
    Set<ContextConfigurator> configurators = new HashSet<>();
    configurators.add(new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.addServlet(new ServletHolder(new PingServlet()), "/*");
      }
    });
    return new WebServerTask(ri, conf, configurators);
  }

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

  @Test
  public void testHttp() throws Exception {
    Configuration conf = new Configuration();
    int httpPort = getRandomPort();
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
      Thread.sleep(1000);
      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort).openConnection();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } finally {
      ws.stopTask();
    }
  }

  //making the url connection to trust a self signed cert on localhost
  private void configureHttpsUrlConnection(HttpsURLConnection conn) throws Exception {
    SSLContext sc = SSLContext.getInstance("SSL");
    TrustManager[] trustAllCerts = new TrustManager[] {
        new X509TrustManager() {
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }
          public void checkClientTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }
          public void checkServerTrusted(
              java.security.cert.X509Certificate[] certs, String authType) {
          }
        }
    };
    sc.init(null, trustAllCerts, new java.security.SecureRandom());
    conn.setSSLSocketFactory(sc.getSocketFactory());
    conn.setHostnameVerifier(new HostnameVerifier() {
      @Override
      public boolean verify(String s, SSLSession sslSession) {
        return true;
      }
    });
  }

  @Test
  public void testHttps() throws Exception {
    Configuration conf = new Configuration();
    int httpsPort = getRandomPort();
    String confDir = createTestDir();
    String keyStore = new File(confDir, "sdc-keystore.jks").getAbsolutePath();
    OutputStream os = new FileOutputStream(keyStore);
    IOUtils.copy(getClass().getClassLoader().getResourceAsStream("sdc-keystore.jks"),os);
    os.close();
    conf.set(WebServerTask.AUTHENTICATION_KEY, "none");
    conf.set(WebServerTask.HTTP_PORT_KEY, 0);
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
      Thread.sleep(1000);
      HttpsURLConnection conn = (HttpsURLConnection) new URL("https://127.0.0.1:" + httpsPort).openConnection();
      configureHttpsUrlConnection(conn);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    } finally {
      ws.stopTask();
    }
  }

  @Test
  public void testHttpRedirectToHttpss() throws Exception {
    Configuration conf = new Configuration();
    int httpPort = getRandomPort();
    int httpsPort = getRandomPort();
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
      Thread.sleep(1000);
      HttpURLConnection conn = (HttpURLConnection) new URL("http://127.0.0.1:" + httpPort).openConnection();
      conn.setInstanceFollowRedirects(false);
      Assert.assertTrue(conn.getResponseCode() >= 300 && conn.getResponseCode() < 400);
      Assert.assertTrue(conn.getHeaderField("Location").startsWith("https://"));
      HttpsURLConnection conns = (HttpsURLConnection) new URL(conn.getHeaderField("Location")).openConnection();
      configureHttpsUrlConnection(conns);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conns.getResponseCode());
    } finally {
      ws.stopTask();
    }
  }

}
