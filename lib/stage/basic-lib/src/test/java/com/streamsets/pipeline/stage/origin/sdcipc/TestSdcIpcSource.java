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
package com.streamsets.pipeline.stage.origin.sdcipc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.destination.sdcipc.Constants;
import com.streamsets.pipeline.stage.util.tls.TLSTestUtils;
import com.streamsets.testing.NetworkUtils;
import org.iq80.snappy.SnappyFramedOutputStream;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
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
  private static int randomPort;

  @BeforeClass
  public static void setUp() throws Exception {
    randomPort = NetworkUtils.getRandomPort();
  }

  private void testReceiveRecords(final boolean ssl, final boolean compressed) throws Exception {
    String hostname = TLSTestUtils.getHostname();
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File keyStore = new File(testDir, "keystore.jks");
    final File trustStore = new File(testDir, "truststore.jks");
    if (ssl) {
      Assert.assertTrue(testDir.mkdirs());
      KeyPair keyPair = TLSTestUtils.generateKeyPair();
      Certificate cert = TLSTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
      TLSTestUtils.createKeyStore(keyStore.toString(), "keystore", "web", keyPair.getPrivate(), cert);
      TLSTestUtils.createTrustStore(trustStore.toString(), "truststore", "web", cert);
    }

    final Configs configs = new Configs();
    configs.appId = () -> "appId";
    configs.tlsConfigBean.tlsEnabled = ssl;
    configs.tlsConfigBean.keyStoreFilePath = keyStore.toString();
    configs.tlsConfigBean.keyStorePassword = () -> "keystore";
    configs.port = randomPort;
    configs.maxWaitTimeSecs = 2;
    Source source = new SdcIpcSource(configs);
    final SourceRunner runner = new SourceRunner.Builder(SdcIpcDSource.class, source).addOutputLane("lane").build();
    try {
      runner.runInit();

      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

      //valid init
      Future<Boolean> future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          HttpURLConnection conn = getConnection(Constants.IPC_PATH, configs.appId, runner.getContext(),
                                                 TLSTestUtils.getHostname() + ":" + configs.port, ssl,
                                                 trustStore.toString(), "truststore");
          conn.setRequestMethod("GET");
          conn.setDefaultUseCaches(false);
          conn.setDoOutput(false);
          return conn.getResponseCode() == HttpURLConnection.HTTP_OK &&
                 Constants.X_SDC_PING_VALUE.equals(conn.getHeaderField(Constants.X_SDC_PING_HEADER));
        }
      });

      Assert.assertTrue(future.get(5, TimeUnit.SECONDS));

      //valid IPC
      future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Record r1 = RecordCreator.create();
          r1.set(Field.create(true));
          Record r2 = RecordCreator.create();
          r2.set(Field.create(false));
          List<Record> records = ImmutableList.of(r1, r2);
          return sendRecords(configs.appId, runner.getContext(), TLSTestUtils.getHostname() + ":" + configs.port, ssl,
                             trustStore.toString(), "truststore", compressed, records);
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
          return sendRecords(() ->"invalid", runner.getContext(), TLSTestUtils.getHostname() + ":" + configs.port, ssl,
                             trustStore.toString(), "truststore", compressed, records);
        }
      });

      Assert.assertFalse(future.get(5, TimeUnit.SECONDS));

      //test PING
      HttpURLConnection conn = getConnection("/ping", () -> "nop", runner.getContext(),
                                             TLSTestUtils.getHostname() + ":" + configs.port, ssl,
                                             trustStore.toString(), "truststore");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    } finally {
      runner.runDestroy();
    }
  }

  private HttpURLConnection getConnection(
    String path,
    CredentialValue appId,
    Stage.Context context,
    String hostPort,
    boolean ssl,
    String trustStoreFile,
    String trustStorePassword
  ) throws Exception {
    com.streamsets.pipeline.stage.destination.sdcipc.Configs config =
        new com.streamsets.pipeline.stage.destination.sdcipc.Configs();
    // always valid to be able to init the config
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 1000;
    config.readTimeOutMs = 1000;
    config.hostPorts = ImmutableList.of(hostPort);
    config.tlsConfigBean.tlsEnabled = ssl;
    config.hostVerification = false;
    config.tlsConfigBean.trustStoreFilePath = trustStoreFile;
    config.tlsConfigBean.trustStorePassword = () -> trustStorePassword;
    config.tlsConfigBean.useDefaultCiperSuites = true;
    config.tlsConfigBean.cipherSuites.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");
    List<Stage.ConfigIssue> issues = config.init(context);
    // now setting the appId we want to test
    config.appId = appId;
    if (issues.isEmpty()) {
        return config.createConnection(hostPort, path);
    } else {
      throw new IOException(issues.toString());
    }
  }

  private boolean sendRecords(
    CredentialValue appId,
    Stage.Context context,
    String hostPort,
    boolean ssl,
    String trustStoreFile,
    String trustStorePassword,
    boolean compressed,
    List<Record> records) throws Exception {
    try {
      ContextExtensions ext = (ContextExtensions) context;
      HttpURLConnection conn = getConnection(Constants.IPC_PATH, appId, context, hostPort, ssl, trustStoreFile,
                                             trustStorePassword);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.CONTENT_TYPE_HEADER, Constants.APPLICATION_BINARY);
      if (compressed) {
        conn.setRequestProperty(Constants.X_SDC_COMPRESSION_HEADER, Constants.SNAPPY_COMPRESSION);
      }
      conn.setDefaultUseCaches(false);
      conn.setDoOutput(true);
      conn.setDoInput(true);
      OutputStream os = conn.getOutputStream();
      if (compressed) {
        os = new SnappyFramedOutputStream(os);
      }
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
    testReceiveRecords(false, false);
    testReceiveRecords(false, true);
  }

  @Test
  public void testReceiveRecordsHttps() throws Exception {
    testReceiveRecords(true, false);
    testReceiveRecords(true, true);
  }

}
