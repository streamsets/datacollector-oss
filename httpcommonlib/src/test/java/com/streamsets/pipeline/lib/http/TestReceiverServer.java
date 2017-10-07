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
package com.streamsets.pipeline.lib.http;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.util.tls.TLSTestUtils;
import com.streamsets.testing.NetworkUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestReceiverServer {

  @Test
  public void testLifecycleHttp() throws Exception {
    final int port = NetworkUtils.getRandomPort();
    HttpConfigs configs = new HttpConfigs("g", "p") {
      @Override
      public int getPort() {
        return port;
      }

      @Override
      public int getMaxConcurrentRequests() {
        return 10;
      }

      @Override
      public CredentialValue getAppId() {
        return () -> "id";
      }

      @Override
      public int getMaxHttpRequestSizeKB() {
        return 1;
      }

      @Override
      public boolean isTlsEnabled() {
        return false;
      }

      @Override
      public boolean isAppIdViaQueryParamAllowed() {
        return false;
      }

      @Override
      public TlsConfigBean getTlsConfigBean() {
        return null;
      }
    };

    HttpReceiver receiver = Mockito.mock(HttpReceiverWithFragmenterWriter.class);
    Mockito.when(receiver.getAppId()).thenReturn(() -> "id");
    Mockito.when(receiver.getUriPath()).thenReturn("/path");
    BlockingQueue<Exception> exQueue = new ArrayBlockingQueue<>(10);

    HttpReceiverServer server = new HttpReceiverServer(configs, receiver, exQueue);

    Assert.assertTrue(server.getJettyServerThreads(1) > 1);
    Assert.assertTrue(server.getJettyServerMaxThreads() > 10);
    Assert.assertTrue(server.getJettyServerMinThreads() >= server.getJettyServerThreads(1));

    Stage.Context context =
        ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    try {
      Assert.assertTrue(server.init(context).isEmpty());

      // valid ping
      HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + port + "/path").openConnection();
      conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "id");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertEquals(HttpConstants.X_SDC_PING_VALUE, conn.getHeaderField(HttpConstants.X_SDC_PING_HEADER));

      // invalid ping
      conn = (HttpURLConnection) new URL("http://localhost:" + port + "/path").openConnection();
      conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "invalid");
      Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());

      // valid post
      Mockito.reset(receiver);
      Mockito.when(receiver.getAppId()).thenReturn(() -> "id");
      Mockito.when(receiver.validate(Mockito.any(HttpServletRequest.class), Mockito.any(HttpServletResponse.class)))
          .thenReturn(true);
      Mockito.when(receiver.process(Mockito.any(), Mockito.any())).thenReturn(true);
      conn = (HttpURLConnection) new URL("http://localhost:" + port + "/path").openConnection();
      conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "id");
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      conn.getOutputStream().write("abc".getBytes());
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Mockito.verify(receiver, Mockito.times(1)).validate(Mockito.any(HttpServletRequest.class), Mockito.any
          (HttpServletResponse.class));
      Mockito.verify(receiver, Mockito.times(1)).process(Mockito.any(HttpServletRequest.class), Mockito.any
          (InputStream.class));

      // invalid post
      Mockito.reset(receiver);
      Mockito.when(receiver.getAppId()).thenReturn(() -> "id");
      Mockito.when(receiver.validate(Mockito.any(HttpServletRequest.class), Mockito.any(HttpServletResponse.class)))
          .thenReturn(false);
      conn = (HttpURLConnection) new URL("http://localhost:" + port + "/path").openConnection();
      conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "id");
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      conn.getOutputStream().write("abc".getBytes());
      conn.getResponseCode();
      Mockito.verify(receiver, Mockito.times(1)).validate(Mockito.any(HttpServletRequest.class), Mockito.any
          (HttpServletResponse.class));
      Mockito.verify(receiver, Mockito.times(0)).process(Mockito.any(HttpServletRequest.class), Mockito.any
          (InputStream.class));
    } finally {
      server.destroy();
    }
  }

  @Test
  public void testLifecycleHttps() throws Exception {
    // setup TLS
    String hostname = TLSTestUtils.getHostname();
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    final File keyStore = new File(testDir, "keystore.jks");
    Assert.assertTrue(testDir.mkdirs());
    final String keyStorePassword = "keystore";
    final File trustStore = new File(testDir, "truststore.jks");
    KeyPair keyPair = TLSTestUtils.generateKeyPair();
    Certificate cert = TLSTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
    TLSTestUtils.createKeyStore(keyStore.toString(), keyStorePassword, "web", keyPair.getPrivate(), cert);
    TLSTestUtils.createTrustStore(trustStore.toString(), "truststore", "web", cert);

    final int port = NetworkUtils.getRandomPort();
    final HttpConfigs configs = new HttpConfigs("g", "p") {

      private TlsConfigBean tlsConfigBean = new TlsConfigBean();

      {
        tlsConfigBean.keyStoreFilePath = keyStore.getAbsolutePath();
        tlsConfigBean.keyStorePassword = () -> keyStorePassword;
        tlsConfigBean.tlsEnabled = true;
      }

      @Override
      public int getPort() {
        return port;
      }

      @Override
      public int getMaxConcurrentRequests() {
        return 10;
      }

      @Override
      public CredentialValue getAppId() {
        return () -> "id";
      }

      @Override
      public int getMaxHttpRequestSizeKB() {
        return 1;
      }

      @Override
      public boolean isTlsEnabled() {
        return tlsConfigBean.isEnabled();
      }

      @Override
      public boolean isAppIdViaQueryParamAllowed() {
        return false;
      }

      @Override
      public TlsConfigBean getTlsConfigBean() {
        return tlsConfigBean;
      }
    };

    HttpReceiver receiver = Mockito.mock(HttpReceiverWithFragmenterWriter.class);
    Mockito.when(receiver.getAppId()).thenReturn(() -> "id");
    Mockito.when(receiver.getUriPath()).thenReturn("/path");
    BlockingQueue<Exception> exQueue = new ArrayBlockingQueue<>(10);

    HttpReceiverServer server = new HttpReceiverServer(configs, receiver, exQueue);

    Assert.assertTrue(server.getJettyServerThreads(1) > 1);
    Assert.assertTrue(server.getJettyServerMaxThreads() > 10);
    Assert.assertTrue(server.getJettyServerMinThreads() >= server.getJettyServerThreads(1));

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    final Stage.Context context =
        ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, ImmutableList.of("a"));
    try {
      Assert.assertTrue(configs.init(context).isEmpty());
      Assert.assertTrue(server.init(context).isEmpty());

      Future<Boolean> future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          HttpURLConnection conn = getConnection("/path",
              configs.getAppId(),
              context,
              TLSTestUtils.getHostname() + ":" + configs.getPort(),
              trustStore.toString(),
              "truststore"
          );
          return conn.getResponseCode() == HttpURLConnection.HTTP_OK &&
              HttpConstants.X_SDC_PING_VALUE.equals(conn.getHeaderField(HttpConstants.X_SDC_PING_HEADER));
        }
      });
      Assert.assertTrue(future.get(5, TimeUnit.SECONDS));
    } finally {
      server.destroy();
      executor.shutdownNow();
    }
  }

  static final HostnameVerifier ACCEPT_ALL_HOSTNAME_VERIFIER = (s, sslSession) -> true;

  private HttpURLConnection getConnection(
      String path,
      CredentialValue appId,
      Stage.Context context,
      String hostPort,
      String trustStoreFile,
      String trustStorePassword
  ) throws Exception {
    URL url = new URL("https://" + hostPort.trim() + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(1000);
    conn.setReadTimeout(1000);
    HttpsURLConnection sslConn = (HttpsURLConnection) conn;
    sslConn.setSSLSocketFactory(createSSLSocketFactory(context, trustStoreFile, trustStorePassword));
    sslConn.setHostnameVerifier(ACCEPT_ALL_HOSTNAME_VERIFIER);
    conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, appId.get());
    return conn;
  }

  private String SSL_CERTIFICATE = "SunX509";
  private String[] SSL_ENABLED_PROTOCOLS = {"TLSv1.2"};

  private SSLSocketFactory createSSLSocketFactory(
      Stage.Context context, String trustStoreFile, String trustStorePassword
  ) throws Exception {
    SSLSocketFactory sslSocketFactory;
    if (trustStoreFile.isEmpty()) {
      sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    } else {
      KeyStore keystore = KeyStore.getInstance("jks");
      try (InputStream is = new FileInputStream(new File(context.getResourcesDirectory(), trustStoreFile))) {
        keystore.load(is, trustStorePassword.toCharArray());
      }

      KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(SSL_CERTIFICATE);
      keyMgrFactory.init(keystore, trustStorePassword.toCharArray());
      KeyManager[] keyManagers = keyMgrFactory.getKeyManagers();

      TrustManager[] trustManagers = new TrustManager[1];
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(SSL_CERTIFICATE);
      trustManagerFactory.init(keystore);
      for (TrustManager trustManager1 : trustManagerFactory.getTrustManagers()) {
        if (trustManager1 instanceof X509TrustManager) {
          trustManagers[0] = trustManager1;
          break;
        }
      }
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManagers, trustManagers, null);
      sslContext.getDefaultSSLParameters().setProtocols(SSL_ENABLED_PROTOCOLS);
      sslSocketFactory = sslContext.getSocketFactory();
    }
    return sslSocketFactory;
  }

}
