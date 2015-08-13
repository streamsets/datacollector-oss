/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import com.streamsets.pipeline.stage.destination.sdcipc.SSLTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestSdcIpcSource {

  private int getRandomPort() throws Exception {
    try (ServerSocket ss = new ServerSocket(0)) {
      return ss.getLocalPort();
    }
  }

  private void testReceiveRecords(final boolean ssl) throws Exception {
    String hostname = SSLTestUtils.getHostname();
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File keyStore = new File(testDir, "keystore.jks");
    final File trustStore = new File(testDir, "truststore.jks");
    if (ssl) {
      Assert.assertTrue(testDir.mkdirs());
      KeyPair keyPair = SSLTestUtils.generateKeyPair();
      Certificate cert = SSLTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
      SSLTestUtils.createKeyStore(keyStore.toString(), "keystore", "web", keyPair.getPrivate(), cert);
      SSLTestUtils.createTrustStore(trustStore.toString(), "truststore", "web", cert);
    }

    final Configs configs = new Configs();
    configs.appId = "appId";
    configs.sslEnabled = ssl;
    configs.keyStoreFile = keyStore.toString();
    configs.keyStorePassword = "keystore";
    configs.port = getRandomPort();
    configs.maxWaitTimeSecs = 2;
    Source source = new SdcIpcSource(configs);
    final SourceRunner runner = new SourceRunner.Builder(SdcIpcSource.class, source).addOutputLane("lane").build();
    try {
      runner.runInit();

      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

      //valid IPC
      Future<Boolean> future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Record r1 = RecordCreator.create();
          r1.set(Field.create(true));
          Record r2 = RecordCreator.create();
          r2.set(Field.create(false));
          List<Record> records = ImmutableList.of(r1, r2);
          return sendRecords(configs.appId, runner.getContext(), SSLTestUtils.getHostname() + ":" + configs.port, ssl,
                             trustStore.toString(), "truststore", records);
        }
      });
      StageRunner.Output output = runner.runProduce(null, 10);
      Assert.assertNotNull(output.getNewOffset());
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());
      Assert.assertTrue(output.getRecords().get("lane").get(0).get().getValueAsBoolean());
      Assert.assertFalse(output.getRecords().get("lane").get(1).get().getValueAsBoolean());

      Assert.assertTrue(future.get(5, TimeUnit.SECONDS));

      //invalid IPC
      future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Record r1 = RecordCreator.create();
          r1.set(Field.create(true));
          Record r2 = RecordCreator.create();
          r2.set(Field.create(false));
          List<Record> records = ImmutableList.of(r1, r2);
          return sendRecords("invalid", runner.getContext(), SSLTestUtils.getHostname() + ":" + configs.port, ssl,
                             trustStore.toString(), "truststore", records);
        }
      });

      Assert.assertFalse(future.get(5, TimeUnit.SECONDS));

      //test PING
      HttpURLConnection conn = getConnection("/ping", "nop", runner.getContext(),
                                             SSLTestUtils.getHostname() + ":" + configs.port, ssl,
                                             trustStore.toString(), "truststore");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    } finally {
      runner.runDestroy();
    }
  }

  private HttpURLConnection getConnection(String path, String appId, Stage.Context context, String hostPort, boolean ssl,
      String trustStoreFile, String trustStorePassword) throws Exception {
    com.streamsets.pipeline.stage.destination.sdcipc.Configs config =
        new com.streamsets.pipeline.stage.destination.sdcipc.Configs();
    // always valid to be able to init the config
    config.appId = "appId";
    config.connectionTimeOutMs = 1000;
    config.readTimeOutMs = 1000;
    config.hostPorts = ImmutableList.of(hostPort);
    config.sslEnabled = ssl;
    config.hostVerification = false;
    config.trustStoreFile = trustStoreFile;
    config.trustStorePassword = trustStorePassword;
    List<Stage.ConfigIssue> issues = config.init(context);
    // now setting the appId we want to test
    config.appId = appId;
    if (issues.isEmpty()) {
        return config.createConnection(hostPort, path);
    } else {
      throw new IOException(issues.toString());
    }
  }

  private boolean sendRecords(String appId, Stage.Context context, String hostPort, boolean ssl, String trustStoreFile,
      String trustStorePassword,  List<Record> records)
      throws Exception {
    try {
      ContextExtensions ext = (ContextExtensions) context;
      HttpURLConnection conn = getConnection(Constants.IPC_PATH, appId, context, hostPort, ssl, trustStoreFile,
                                             trustStorePassword);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.CONTENT_TYPE_HEADER, Constants.APPLICATION_BINARY);
      conn.setDefaultUseCaches(false);
      conn.setDoOutput(true);
      conn.setDoInput(true);
      OutputStream os = conn.getOutputStream();
      RecordWriter writer = ext.createRecordWriter(os);
      for (Record record : records) {
        writer.write(record);
      }
      writer.close();
      return conn.getResponseCode() == HttpURLConnection.HTTP_OK;
    } catch (Exception ex) {
      System.out.println(ex);
      throw ex;
    }
  }

  @Test
  public void testReceiveRecordsHttp() throws Exception {
    testReceiveRecords(false);
  }

  @Test
  public void testReceiveRecordsHttps() throws Exception {
    testReceiveRecords(true);
  }

}
