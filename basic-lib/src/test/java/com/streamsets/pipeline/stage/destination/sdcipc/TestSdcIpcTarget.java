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
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.util.tls.TLSTestUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestSdcIpcTarget {

  @Test
  public void testSingleHost() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    Configs config = new ForTestConfigs(conn);
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 100;
    config.readTimeOutMs = 200;
    config.hostPorts = ImmutableList.of("localhost:10000");
    config.retriesPerBatch = 2;
    config.tlsConfigBean.tlsEnabled = false;
    config.tlsConfigBean.trustStoreFilePath = "";
    config.tlsConfigBean.trustStorePassword = () -> "";
    config.hostVerification = true;
    SdcIpcTarget target = new SdcIpcTarget(config);
    target.initializeHostPortsLists();

    Assert.assertEquals("localhost:10000", target.getHostPort(false));
    Assert.assertEquals("localhost:10000", target.getHostPort(true));
    Assert.assertEquals("localhost:10000", target.getHostPort(false));
  }

  @Test
  public void testMultipleHosts() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    Configs config = new ForTestConfigs(conn);
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 100;
    config.readTimeOutMs = 200;
    config.hostPorts = Arrays.asList("localhost:10000", "localhost:10001");
    config.retriesPerBatch = 2;
    config.tlsConfigBean.tlsEnabled = false;
    config.tlsConfigBean.trustStoreFilePath = "";
    config.tlsConfigBean.trustStorePassword = () -> "";
    config.hostVerification = true;

    // 2 hostPorts

    SdcIpcTarget target = new SdcIpcTarget(config);
    target.initializeHostPortsLists();

    Assert.assertEquals(2, target.getActiveConnectionsNumber());

    String h1 = target.getHostPort(false);
    String h2 = target.getHostPort(false);
    Assert.assertNotEquals(h1, h2);
    String h3 = target.getHostPort(false);
    Assert.assertEquals(h1, h3);

    String h4 = target.getHostPort(true);
    Assert.assertNotEquals(h3, h4);

    String h5 = target.getHostPort(true);
    Assert.assertNotEquals(h4, h5);

    // 10 hostPorts

    List<String> hostPorts = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      hostPorts.add("h:" + (i + 1));
    }
    config.hostPorts = hostPorts;
    target = new SdcIpcTarget(config);
    target.initializeHostPortsLists();

    Assert.assertEquals(3, target.getActiveConnectionsNumber());

    // test round robin
    for (String hostPort : target.activeHostPorts) {
      Assert.assertEquals(hostPort, target.getHostPort(false));
    }
    for (String hostPort : target.activeHostPorts) {
      Assert.assertEquals(hostPort, target.getHostPort(false));
    }

    // test hostport replacement on error
    int currentIdx = target.lastActive;
    String firstStandBy = target.standByHostPorts.get(0);

    Assert.assertEquals(firstStandBy, target.getHostPort(true));
    Assert.assertEquals(currentIdx, target.lastActive);
  }


  @Test
  public void testWriteOK() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    Configs config = new ForTestConfigs(conn);
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 100;
    config.readTimeOutMs = 200;
    config.hostPorts = ImmutableList.of("localhost:10000");
    config.retriesPerBatch = 2;
    config.tlsConfigBean.tlsEnabled = false;
    config.tlsConfigBean.trustStoreFilePath = "";
    config.tlsConfigBean.trustStorePassword = () -> "";
    config.hostVerification = true;

    SdcIpcTarget target = new SdcIpcTarget(config);

    TargetRunner runner = new TargetRunner.Builder(SdcIpcDTarget.class, target).build();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    Mockito.when(conn.getOutputStream()).thenReturn(baos);

    try {
      runner.runInit();
      List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      ContextExtensions ext = (ContextExtensions) runner.getContext();
      RecordReader recordReader = ext.createRecordReader(new ByteArrayInputStream(baos.toByteArray()), 0, -1);
      Assert.assertNotNull(recordReader.readRecord());
      Assert.assertNotNull(recordReader.readRecord());
      Assert.assertNull(recordReader.readRecord());
      recordReader.close();
    } finally {
      runner.runDestroy();
    }
  }

  private void testWriteError(boolean connectionError, boolean badResponse) throws Exception {
    int writeResponseCode = (badResponse) ? HttpURLConnection.HTTP_BAD_REQUEST : HttpURLConnection.HTTP_OK;

    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    Configs config = new ForTestConfigs(conn);
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 100;
    config.readTimeOutMs = 200;
    config.hostPorts = ImmutableList.of("localhost:10000");
    config.retriesPerBatch = 2;
    config.tlsConfigBean.tlsEnabled = false;
    config.tlsConfigBean.trustStoreFilePath = "";
    config.tlsConfigBean.trustStorePassword = () -> "";
    config.hostVerification = true;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    Mockito.when(conn.getOutputStream()).thenReturn(baos);

    SdcIpcTarget target = new SdcIpcTarget(config);

    // RECORDS TO ERROR
    TargetRunner runner = new TargetRunner.Builder(SdcIpcDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR)
                                                                              .build();
    try {
      runner.runInit();

      Mockito.when(conn.getResponseCode()).thenReturn(writeResponseCode);
      if (connectionError) {
        Mockito.when(conn.getOutputStream()).thenThrow(new IOException());
      }

      List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
      runner.runWrite(records);
      Assert.assertEquals(2, runner.getErrorRecords().size());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }

    // DISCARD RECORDS
    Mockito.reset(conn);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    Mockito.when(conn.getOutputStream()).thenReturn(baos);
    runner = new TargetRunner.Builder(SdcIpcDTarget.class, target).setOnRecordError(OnRecordError.DISCARD).build();
    try {
      runner.runInit();

      Mockito.when(conn.getResponseCode()).thenReturn(writeResponseCode);
      if (connectionError) {
        Mockito.when(conn.getOutputStream()).thenThrow(new IOException());
      }

      List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }

    // STOP PIPELINE
    Mockito.reset(conn);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    Mockito.when(conn.getOutputStream()).thenReturn(baos);
    runner = new TargetRunner.Builder(SdcIpcDTarget.class, target).setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    try {
      runner.runInit();

      Mockito.when(conn.getResponseCode()).thenReturn(writeResponseCode);
      if (connectionError) {
        Mockito.when(conn.getOutputStream()).thenThrow(new IOException());
      }

      List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
      runner.runWrite(records);
      Assert.fail();
    } catch (StageException ex) {
      //NOP
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWriteConnectionError() throws Exception {
    testWriteError(true, false);
  }

  @Test
  public void testWriteHttpResponseError() throws Exception {
    testWriteError(false, true);
  }

  private static class ReceiverServlet extends HttpServlet {
    boolean compressedData;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      String appId = req.getHeader(Constants.X_SDC_APPLICATION_ID_HEADER);
      if (appId == null || !appId.equals("appId")) {
        resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
      } else {
        resp.setHeader(Constants.X_SDC_PING_HEADER, Constants.X_SDC_PING_VALUE);
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      String appId = req.getHeader(Constants.X_SDC_APPLICATION_ID_HEADER);
      String contentType = req.getContentType();
      if (contentType == null || !contentType.equals(Constants.APPLICATION_BINARY)) {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      } else if (appId == null || !appId.equals("appId")) {
        resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
      } else {
        compressedData = req.getHeader(Constants.X_SDC_COMPRESSION_HEADER) != null &&
                         req.getHeader(Constants.X_SDC_COMPRESSION_HEADER).equals(Constants.SNAPPY_COMPRESSION);
        InputStream is = req.getInputStream();
        while (is.read() > 1);
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    }

  }

  @Test
  public void testHttp() throws Exception {
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    ReceiverServlet servlet = new ReceiverServlet();
    context.addServlet(new ServletHolder(servlet), Constants.IPC_PATH);
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();

      Configs config = new Configs();
      config.appId = () -> "appId";
      config.connectionTimeOutMs = 1000;
      config.readTimeOutMs = 2000;
      config.hostPorts = ImmutableList.of("localhost:" + server.getURI().getPort());
      config.retriesPerBatch = 2;
      config.tlsConfigBean.tlsEnabled = false;
      config.tlsConfigBean.trustStoreFilePath = "";
      config.tlsConfigBean.trustStorePassword = () -> "";
      config.hostVerification = true;
      config.compression = false;

      // test OK without compression
      SdcIpcTarget target = new SdcIpcTarget(config);

      TargetRunner runner = new TargetRunner.Builder(SdcIpcDTarget.class, target)
          .setOnRecordError(OnRecordError.TO_ERROR).build();
      try {
        runner.runInit();
        List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
        runner.runWrite(records);
        Assert.assertFalse(servlet.compressedData);
        Assert.assertTrue(runner.getErrorRecords().isEmpty());
        Assert.assertTrue(runner.getErrors().isEmpty());
      } finally {
        runner.runDestroy();
      }

      // test OK with compression

      config.compression = false;
      target = new SdcIpcTarget(config);
      runner = new TargetRunner.Builder(SdcIpcDTarget.class, target)
          .setOnRecordError(OnRecordError.TO_ERROR).build();
      try {
        runner.runInit();
        List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
        runner.runWrite(records);
        Assert.assertFalse(servlet.compressedData);
        Assert.assertTrue(runner.getErrorRecords().isEmpty());
        Assert.assertTrue(runner.getErrors().isEmpty());
      } finally {
        runner.runDestroy();
      }

      // test ERROR

      target = new SdcIpcTarget(config);

      runner = new TargetRunner.Builder(SdcIpcDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
      try {
        runner.runInit();

        // to force the error
        config.appId = () -> "invalid";

        List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
        runner.runWrite(records);
        Assert.assertEquals(2, runner.getErrorRecords().size());
        Assert.assertTrue(runner.getErrors().isEmpty());
      } finally {
        runner.runDestroy();
      }

    } finally {
      server.stop();
    }
  }

  private void testHttps(boolean hostVerification) throws Exception {
    String hostname = (hostVerification) ? TLSTestUtils.getHostname() : "localhost";

    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    KeyPair keyPair = TLSTestUtils.generateKeyPair();
    Certificate cert = TLSTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
    File keyStore = new File(testDir, "keystore.jks");
    TLSTestUtils.createKeyStore(keyStore.toString(), "keystore", "web", keyPair.getPrivate(), cert);
    File trustStore = new File(testDir, "truststore.jks");
    TLSTestUtils.createTrustStore(trustStore.toString(), "truststore", "web", cert);

    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    context.addServlet(new ServletHolder(new ReceiverServlet()), Constants.IPC_PATH);
    context.setContextPath("/");
    server.setHandler(context);

    //Create a connector for HTTPS
    HttpConfiguration httpsConf = new HttpConfiguration();
    httpsConf.addCustomizer(new SecureRequestCustomizer());
    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keyStore.getPath());
    sslContextFactory.setKeyStorePassword("keystore");
    sslContextFactory.setKeyManagerPassword("keystore");
    ServerConnector httpsConnector = new ServerConnector(server,
                                                         new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                                         new HttpConnectionFactory(httpsConf));
    httpsConnector.setPort(0);
    server.setConnectors(new Connector[]{httpsConnector});

    try {
      server.start();

      Configs config = new Configs();
      config.appId = () -> "appId";
      config.connectionTimeOutMs = 1000;
      config.readTimeOutMs = 2000;
      config.hostPorts = ImmutableList.of(hostname + ":" + server.getURI().getPort());
      config.retriesPerBatch = 2;
      config.tlsConfigBean.tlsEnabled = true;
      config.tlsConfigBean.trustStoreFilePath = trustStore.getName();
      config.tlsConfigBean.trustStorePassword = () -> "truststore";
      config.hostVerification = hostVerification;

      SdcIpcTarget target = new SdcIpcTarget(config);

      TargetRunner runner = new TargetRunner.Builder(SdcIpcDTarget.class, target)
          .setOnRecordError(OnRecordError.TO_ERROR).setResourcesDir(testDir.toString()).build();
      try {
        runner.runInit();
        List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
        runner.runWrite(records);
        Assert.assertTrue(runner.getErrorRecords().isEmpty());
        Assert.assertTrue(runner.getErrors().isEmpty());
      } finally {
        runner.runDestroy();
      }

    } finally {
      server.stop();
    }
  }

  @Test
  public void testHttpsWithHostVerification() throws Exception {
    testHttps(true);
  }

  @Test
  public void testHttpsWithoutHostVerification() throws Exception {
    testHttps(false);
  }

  @Test
  public void testBackOff() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    Configs config = new ForTestConfigs(conn);
    config.appId = () -> "appId";
    config.hostPorts = ImmutableList.of("localhost:10000");
    config.retriesPerBatch = 3;
    config.backOff = 10;
    config.tlsConfigBean.tlsEnabled = false;
    config.hostVerification = true;

    SdcIpcTarget target = new SdcIpcTarget(config);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    Mockito.when(conn.getOutputStream()).thenReturn(baos);

    TargetRunner runner = new TargetRunner.Builder(SdcIpcDTarget.class, target)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();

    try {
      runner.runInit();

      Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
      Mockito.when(conn.getOutputStream()).thenThrow(new IOException());

      List<Record> records = ImmutableList.of(RecordCreator.create(), RecordCreator.create());
      long startTime = System.currentTimeMillis();
      runner.runWrite(records);
      long runningTime = System.currentTimeMillis() - startTime;

      // All records
      Assert.assertEquals(2, runner.getErrorRecords().size());
      Assert.assertTrue(runner.getErrors().isEmpty());
      // The backoff is exactly 1100 ms - 10*10*10 (3 retries per bach)
      Assert.assertTrue(runningTime > 1000);
    } finally {
      runner.runDestroy();
    }
  }


}
