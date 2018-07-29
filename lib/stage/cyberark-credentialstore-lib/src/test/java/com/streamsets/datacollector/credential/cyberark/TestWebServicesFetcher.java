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
package com.streamsets.datacollector.credential.cyberark;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.bouncycastle.x509.X509V1CertificateGenerator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.x500.X500Principal;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

public class TestWebServicesFetcher {
  private File confDir;

  @Before
  public void setup() throws IOException {
    confDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(getConfDir().mkdirs());
    File basicFile = new File(getConfDir(), "basic.properties");
    try (Writer writer = new FileWriter(basicFile)) {
      writer.write("admin:   MD5:21232f297a57a5a743894a0e4a801fc3,user,admin\n");
    }
    File digestFile = new File(getConfDir(), "digest.properties");
    try (Writer writer = new FileWriter(digestFile)) {
      writer.write("admin:   MD5:184b0de86a7c6e86924b5978c97d6969,user,admin\n");
    }

    System.setProperty("sdc.conf.dir", getConfDir().getAbsolutePath());
//    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
//    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "DEBUG");
  }

  @After
  public void destroy() {
    System.getProperties().remove("sdc.conf.dir");
  }

  private Configuration createConfig(Properties properties) {
    CredentialStore.Context context = new CredentialStore.Context() {
      @Override
      public String getId() {
        return "cyberark";
      }

      @Override
      public CredentialStore.ConfigIssue createConfigIssue(ErrorCode errorCode, Object... objects) {
        return new CredentialStore.ConfigIssue() {
          @Override
          public String toString() {
            return errorCode.toString();
          }
        };
      }

      @Override
      public String getConfig(String s) {
        return properties.getProperty(s);
      }
    };
    return new Configuration(context);
  }

  @Test
  public void testInitializationDefaults() throws Exception {
    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://foo");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);
      Assert.assertNotNull(fetcher.getConfig());

      Assert.assertEquals("http://foo", fetcher.getUrl());
      Assert.assertEquals("appId", fetcher.getAppId());
      Assert.assertEquals(WebServicesFetcher.CONNECTION_TIMEOUT_DEFAULT, fetcher.getConnectionTimeout());
      Assert.assertEquals(WebServicesFetcher.NAME_SEPARATOR_DEFAULT, fetcher.getSeparator());
      Assert.assertEquals(WebServicesFetcher.HTTP_AUTH_NONE, fetcher.getHttpAuth());
      Assert.assertNull(fetcher.getCredentialsProvider());
      Assert.assertNull(fetcher.getAuthCache());

      PoolingHttpClientConnectionManager connectionManager = fetcher.getConnectionManager();
      Assert.assertEquals(WebServicesFetcher.MAX_CONCURRENT_CONNECTIONS_DEFAULT, connectionManager.getMaxTotal());
      Assert.assertEquals(WebServicesFetcher.MAX_CONCURRENT_CONNECTIONS_DEFAULT,
          connectionManager.getDefaultMaxPerRoute()
      );
      Assert.assertEquals(WebServicesFetcher.VALIDATE_AFTER_INACTIVITY_DEFAULT,
          connectionManager.getValidateAfterInactivity()
      );

      Assert.assertNull(fetcher.getSslConnectionSocketFactory());
    } finally {
      fetcher.destroy();
    }
  }

  @Test
  public void testInitializationCustomNoSslBasicAuth() throws Exception {
    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://foo");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.KEYSTORE_FILE_KEY, "");
    props.setProperty(WebServicesFetcher.KEYSTORE_PASSWORD_KEY, "");
    props.setProperty(WebServicesFetcher.KEY_PASSWORD_KEY, "");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "");
    props.setProperty(WebServicesFetcher.SUPPORTED_PROTOCOLS_KEY, "");
    props.setProperty(WebServicesFetcher.HOSTNAME_VERIFIER_SKIP_KEY, "");
    props.setProperty(WebServicesFetcher.MAX_CONCURRENT_CONNECTIONS_KEY, "1");
    props.setProperty(WebServicesFetcher.VALIDATE_AFTER_INACTIVITY_KEY, "2");
    props.setProperty(WebServicesFetcher.CONNECTION_TIMEOUT_KEY, "5000");
    props.setProperty(WebServicesFetcher.NAME_SEPARATOR_KEY, "+");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_TYPE_KEY, "basic");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_USER_KEY, "user");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_PASSWORD_KEY, "password");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);
      Assert.assertNotNull(fetcher.getConfig());

      Assert.assertEquals("http://foo", fetcher.getUrl());
      Assert.assertEquals("appId", fetcher.getAppId());
      Assert.assertEquals(5000, fetcher.getConnectionTimeout());
      Assert.assertEquals("+", fetcher.getSeparator());
      Assert.assertEquals("basic", fetcher.getHttpAuth());
      Assert.assertEquals("user", fetcher.getHttpAuthUser());
      Assert.assertEquals("password", fetcher.getHttpAuthPassword());
      Assert.assertNotNull(fetcher.getCredentialsProvider());
      Assert.assertEquals("user",
          fetcher.getCredentialsProvider().getCredentials(AuthScope.ANY).getUserPrincipal().getName()
      );
      Assert.assertEquals("password", fetcher.getCredentialsProvider().getCredentials(AuthScope.ANY).getPassword());
      Assert.assertTrue(fetcher.getAuthCache().get(new HttpHost(fetcher.getUrl())) instanceof BasicScheme);

      PoolingHttpClientConnectionManager connectionManager = fetcher.getConnectionManager();
      Assert.assertEquals(1, connectionManager.getMaxTotal());
      Assert.assertEquals(1, connectionManager.getDefaultMaxPerRoute());
      Assert.assertEquals(2, connectionManager.getValidateAfterInactivity());

      Assert.assertNull(fetcher.getSslConnectionSocketFactory());
    } finally {
      fetcher.destroy();
    }
  }

  @Test
  public void testInitializationCustomNoSslDigestAuth() throws Exception {
    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://foo");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.KEYSTORE_FILE_KEY, "");
    props.setProperty(WebServicesFetcher.KEYSTORE_PASSWORD_KEY, "");
    props.setProperty(WebServicesFetcher.KEY_PASSWORD_KEY, "");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "");
    props.setProperty(WebServicesFetcher.SUPPORTED_PROTOCOLS_KEY, "");
    props.setProperty(WebServicesFetcher.HOSTNAME_VERIFIER_SKIP_KEY, "");
    props.setProperty(WebServicesFetcher.MAX_CONCURRENT_CONNECTIONS_KEY, "1");
    props.setProperty(WebServicesFetcher.VALIDATE_AFTER_INACTIVITY_KEY, "2");
    props.setProperty(WebServicesFetcher.CONNECTION_TIMEOUT_KEY, "5000");
    props.setProperty(WebServicesFetcher.NAME_SEPARATOR_KEY, "+");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_TYPE_KEY, "digest");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_USER_KEY, "user");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_PASSWORD_KEY, "password");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);
      Assert.assertNotNull(fetcher.getConfig());

      Assert.assertEquals("http://foo", fetcher.getUrl());
      Assert.assertEquals("appId", fetcher.getAppId());
      Assert.assertEquals(5000, fetcher.getConnectionTimeout());
      Assert.assertEquals("+", fetcher.getSeparator());
      Assert.assertEquals("digest", fetcher.getHttpAuth());
      Assert.assertEquals("user", fetcher.getHttpAuthUser());
      Assert.assertEquals("password", fetcher.getHttpAuthPassword());
      Assert.assertNotNull(fetcher.getCredentialsProvider());
      Assert.assertEquals("user",
          fetcher.getCredentialsProvider().getCredentials(AuthScope.ANY).getUserPrincipal().getName()
      );
      Assert.assertEquals("password", fetcher.getCredentialsProvider().getCredentials(AuthScope.ANY).getPassword());
      Assert.assertTrue(fetcher.getAuthCache().get(new HttpHost(fetcher.getUrl())) instanceof DigestScheme);

      PoolingHttpClientConnectionManager connectionManager = fetcher.getConnectionManager();
      Assert.assertEquals(1, connectionManager.getMaxTotal());
      Assert.assertEquals(1, connectionManager.getDefaultMaxPerRoute());
      Assert.assertEquals(2, connectionManager.getValidateAfterInactivity());

      Assert.assertNull(fetcher.getSslConnectionSocketFactory());
    } finally {
      fetcher.destroy();
    }
  }

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  public File getConfDir() {
    return confDir;
  }

  public static class MockCyberArkServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      boolean error = false;
      String appId = req.getParameter("AppID");
      if (appId != null) {
        resp.setHeader("pAppID", appId);
      }
      if (appId == null || !appId.equals("appId")) {
        resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
        error = true;
      }

      String safe = req.getParameter("Safe");
      if (safe != null) {
        resp.setHeader("pSafe", safe);
      }
      if (!error && (safe == null || !safe.equals("safe"))) {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        error = true;
      }

      String folder = req.getParameter("Folder");
      if (folder != null) {
        resp.setHeader("pFolder", folder);
      }
      if (!error && (folder != null && !folder.equals("folder"))) {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        error = true;
      }

      String object = req.getParameter("Object");
      if (object != null) {
        resp.setHeader("pObject", object);
      }
      if (!error && (object == null || !object.equals("object"))) {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        error = true;
      }

      String connectionTimeout = req.getParameter("ConnectionTimeout");
      if (!error && (connectionTimeout == null || !connectionTimeout.equals("12345"))) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        error = true;
      }

      String failRequestOnPasswordChange = req.getParameter("FailRequestOnPasswordChange");
      if (!error && (failRequestOnPasswordChange == null || !failRequestOnPasswordChange.equals("true"))) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        error = true;
      }

      resp.setContentType("application/json");
      if (error) {
        resp.getWriter().write("{\"ErrorCode\":\"error\",\"ErrorMsg\":\"message\"}");
      } else {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().write("{\"Content\":\"password\",\"UserName\":\"user\"}");
      }
    }
  }

  protected void runServer(int port, String httpAuth, Callable<Void> test) throws Exception {
    runServer(port, false, false, httpAuth, test);
  }

  protected void runSslServer(int port, boolean clientSsl, Callable<Void> test) throws Exception {
    runServer(port, true, clientSsl, "none", test);
  }

  protected SslContextFactory createSslContextFactory(boolean clientAuth) {
    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(new File(getConfDir(), "serverKS.jks").getAbsolutePath());
    sslContextFactory.setKeyStorePassword("serverKSPassword");
    sslContextFactory.setKeyManagerPassword("serverKeyPassword");
    sslContextFactory.setTrustStorePath(new File(getConfDir(), "trustKS.jks").getAbsolutePath());
    sslContextFactory.setTrustStorePassword("trustKSPassword");
    if (clientAuth) {
      sslContextFactory.setNeedClientAuth(true);
    }
    return sslContextFactory;
  }

  protected Server createServer(int port, boolean serverSsl, boolean clientSsl) {
    Server server = new Server();
    if (!serverSsl) {
      InetSocketAddress addr = new InetSocketAddress("localhost", port);
      ServerConnector connector = new ServerConnector(server);
      connector.setHost(addr.getHostName());
      connector.setPort(addr.getPort());
      server.setConnectors(new Connector[]{connector});
    } else {
      SslContextFactory sslContextFactory = createSslContextFactory(clientSsl);
      ServerConnector httpsConnector = new ServerConnector(server,
          new SslConnectionFactory(sslContextFactory, "http/1.1"),
          new HttpConnectionFactory()
      );
      httpsConnector.setPort(port);
      httpsConnector.setHost("localhost");
      server.setConnectors(new Connector[]{httpsConnector});
    }
    return server;
  }


  protected void runServer(int port, boolean serverSsl, boolean clientSsl, String httpAuth, Callable<Void> test)
      throws Exception {
    Server server = createServer(port, serverSsl, clientSsl);

    ServletContextHandler contextHandler = new ServletContextHandler();
    if (!httpAuth.equals("none")) {
      File realmFile = new File(getConfDir(), httpAuth + ".properties");
      LoginService loginService = new HashLoginService(httpAuth, realmFile.getAbsolutePath());
      server.addBean(loginService);
      ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
      switch (httpAuth) {
        case "basic":
          securityHandler.setAuthenticator(new BasicAuthenticator());
          break;
        case "digest":
          securityHandler.setAuthenticator(new DigestAuthenticator());
          break;
      }
      securityHandler.setLoginService(loginService);
      Constraint constraint = new Constraint();
      constraint.setName("auth");
      constraint.setAuthenticate(true);
      constraint.setRoles(new String[]{"user"});
      ConstraintMapping mapping = new ConstraintMapping();
      mapping.setPathSpec("/*");
      mapping.setConstraint(constraint);
      securityHandler.addConstraintMapping(mapping);
      contextHandler.setSecurityHandler(securityHandler);
    }

    MockCyberArkServlet servlet = new MockCyberArkServlet();
    contextHandler.addServlet(new ServletHolder(servlet), "/AIMWebService/api/Accounts");
    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);
    try {
      server.start();
      test.call();
    } finally {
      server.stop();
    }
  }

  @Test
  public void testWebServiceCallOk() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "none", () -> {
        String got = fetcher.fetch("g",
            "safe&folder&object",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertEquals("password", got);
        got = fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertEquals("password", got);
        got = fetcher.fetch("g",
            "safe&&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertEquals("password", got);
        got = fetcher.fetch("g",
            "safe&folder&object&UserName",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertEquals("user", got);
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test
  public void testWebServiceCallNotFoundBadRequest() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "none", () -> {
        try {
          fetcher.fetch("g",
              "invalid&folder&object",
              ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
          );
          Assert.fail();
        } catch (StageException ex) {
          //expected
        }
        try {
          fetcher.fetch("g",
              "safe&invalid&object",
              ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
          );
          Assert.fail();
        } catch (StageException ex) {
          //expected
        }
        try {
          fetcher.fetch("g",
              "safe&folder&invalid",
              ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
          );
          Assert.fail();
        } catch (StageException ex) {
          //expected
        }
        try {
          fetcher.fetch("g", "safe&&invalid", ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345"));
          Assert.fail();
        } catch (StageException ex) {
          //expected
        }
        try {
          fetcher.fetch("g",
              "safe&folder&object&Content",
              ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "invalid")
          );
          Assert.fail();
        } catch (StageException ex) {
          //expected
        }
        return null;
      });

    } finally {
      fetcher.destroy();
    }

  }

  @Test(expected = StageException.class)
  public void testWebServiceCallForbidden() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "invalid");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "none", () -> {
        String got = fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertNull(got);
        return null;
      });

    } finally {
      fetcher.destroy();
    }

  }

  @Test
  public void testWebServiceCallHttpBasicAuthOk() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_TYPE_KEY, "basic");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_USER_KEY, "admin");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_PASSWORD_KEY, "admin");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "basic", () -> {
        String got = fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertEquals("password", got);
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testWebServiceCallHttpBasicAuthFail() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_TYPE_KEY, "basic");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_USER_KEY, "admin");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_PASSWORD_KEY, "invalid");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "basic", () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test
  public void testWebServiceCallHttpDigestAuthOk() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_TYPE_KEY, "digest");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_USER_KEY, "admin");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_PASSWORD_KEY, "admin");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "basic", () -> {
        String got = fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        Assert.assertEquals("password", got);
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testWebServiceCallHttpDigestAuthFail() throws Exception {
    int port = getFreePort();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "http://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_TYPE_KEY, "digest");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_USER_KEY, "admin");
    props.setProperty(WebServicesFetcher.HTTP_AUTH_PASSWORD_KEY, "invalid");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runServer(port, "basic", () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }


  // SSL setup


  @SuppressWarnings("deprecation")
  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   */ public static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
      throws CertificateEncodingException, InvalidKeyException, IllegalStateException, NoSuchProviderException,
      NoSuchAlgorithmException, SignatureException {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000l);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);

    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }

  private static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, File file, String password)
      throws GeneralSecurityException, IOException {
    FileOutputStream out = new FileOutputStream(file);
    try {
      ks.store(out, password.toCharArray());
    } finally {
      out.close();
    }
  }

  /**
   * Creates a keystore with a single key and saves it to a file.
   *
   * @param file String file to save
   * @param ksPassword String store password to set on keystore
   * @param keyPassword String key password to set on key
   * @param alias String alias to use for the key
   * @param privateKey Key to save in keystore
   * @param cert Certificate to use as certificate chain associated to key
   * @throws GeneralSecurityException for any error with the security APIs
   * @throws IOException if there is an I/O error saving the file
   */
  private static void createKeyStore(
      File file, String ksPassword, String keyPassword, String alias, Key privateKey, Certificate cert
  ) throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(), new Certificate[]{cert});
    saveKeyStore(ks, file, ksPassword);
  }

  private static <T extends Certificate> void createTrustStore(File file, String password, Map<String, T> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, file, password);
  }


  private void createSslConfig() throws Exception {
    File clientKS = new File(getConfDir(), "clientKS.jks");
    String clientKSPassword = "clientKSPassword";
    String clientKeyPassword = "clientKeyPassword";
    File serverKS = new File(getConfDir(), "serverKS.jks");
    String serverKSPassword = "serverKSPassword";
    String serverKeyPassword = "serverKeyPassword";
    File trustKS = new File(getConfDir(), "trustKS.jks");
    String trustPassword = "trustKSPassword";

    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();

    KeyPair sKP = generateKeyPair("RSA");
    X509Certificate sCert = generateCertificate("CN=localhost, O=server", sKP, 30, "SHA1withRSA");
    createKeyStore(serverKS, serverKSPassword, serverKeyPassword, "server", sKP.getPrivate(), sCert);
    certs.put("server", sCert);

    KeyPair cKP = generateKeyPair("RSA");
    X509Certificate cCert = generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA");
    createKeyStore(clientKS, clientKSPassword, clientKeyPassword, "client", cKP.getPrivate(), cCert);
    certs.put("client", cCert);

    createTrustStore(trustKS, trustPassword, certs);
  }

  @Test
  public void testSslServerHostNameVerifierEnabledOK() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "trustKS.jks");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "trustKSPassword");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, false, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testSslServerHostNameVerifierEnabledFail() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://127.0.0.1:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "trustKS.jks");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "trustKSPassword");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, false, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test
  public void testSslServerHostNameVerifierDisabled() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://127.0.0.1:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "trustKS.jks");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "trustKSPassword");
    props.setProperty(WebServicesFetcher.HOSTNAME_VERIFIER_SKIP_KEY, "true");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, false, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testSslServerNoTrustStoreFail() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, false, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testSslServerProtocolSettingFail() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://127.0.0.1:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "trustKS.jks");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "trustKSPassword");
    props.setProperty(WebServicesFetcher.SUPPORTED_PROTOCOLS_KEY, "invalid");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, true, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test
  public void testSslServerClientCertOk() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.KEYSTORE_FILE_KEY, "clientKS.jks");
    props.setProperty(WebServicesFetcher.KEYSTORE_PASSWORD_KEY, "clientKSPassword");
    props.setProperty(WebServicesFetcher.KEY_PASSWORD_KEY, "clientKeyPassword");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "trustKS.jks");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "trustKSPassword");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, true, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }

  @Test(expected = StageException.class)
  public void testSslServerClientCertFail() throws Exception {
    int port = getFreePort();

    createSslConfig();

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, "https://localhost:" + port + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, "appId");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_FILE_KEY, "trustKS.jks");
    props.setProperty(WebServicesFetcher.TRUSTSTORE_PASSWORD_KEY, "trustKSPassword");
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      runSslServer(port, true, () -> {
        fetcher.fetch("g",
            "safe&folder&object&Content",
            ImmutableMap.of(WebServicesFetcher.CONNECTION_CS_TIMEOUT_PARAM, "12345")
        );
        return null;
      });

    } finally {
      fetcher.destroy();
    }
  }


  // To run set the cyberark.vm.enabled system property to TRUE
  // The 'cyberark.vm.url' and the 'cyberark.vm.appId' must be set to the corresponding CyberArk VM settings.
  // The VM setup should have a 'Test' vault with a '\\level1' folder with a 'fu' object with '1234567890' as Content
  // and 'sa' as UserName.
  @Test
  public void testCyberArkCentralCredentialProviderVM() throws Exception {
    Assume.assumeTrue(Boolean.parseBoolean(System.getProperty("cyberark.vm.enabled", "false")));
    String cyberArkUrl = System.getProperty("cyberark.vm.url");
    String appId = System.getProperty("cyberark.vm.appId");
    Assert.assertNotNull(cyberArkUrl, "System property 'cyberark.vm.url' not set");
    Assert.assertNotNull(appId, "System property 'cyberark.vm.appId' not set");

    Properties props = new Properties();
    props.setProperty(WebServicesFetcher.URL_KEY, cyberArkUrl + "/AIMWebService/api/Accounts");
    props.setProperty(WebServicesFetcher.APP_ID_KEY, appId);
    Configuration conf = createConfig(props);

    WebServicesFetcher fetcher = new WebServicesFetcher();
    try {
      fetcher.init(conf);

      String data = fetcher.fetch("g", "Test&Root\\level1&fu", Collections.emptyMap());
      Assert.assertEquals("1234567890", data);

      data = fetcher.fetch(
          "g",
          "Test@Root\\level1@fu@UserName",
          ImmutableMap.of(WebServicesFetcher.CONNECTION_TIMEOUT_PARAM, "3000", "separator", "@")
      );
      Assert.assertEquals("sa", data);

      try {
        fetcher.fetch("g", "Test&&fu", Collections.emptyMap());
        Assert.fail();
      } catch (StageException ex) {
        Assert.assertTrue(ex.toString().contains("404"));
      }

    } finally {
      fetcher.destroy();
    }
  }
}
