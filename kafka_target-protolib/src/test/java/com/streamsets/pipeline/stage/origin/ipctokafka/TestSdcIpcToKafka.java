/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.testing.NetworkUtils;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.TestUtils;
import org.iq80.snappy.SnappyFramedOutputStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Category(SingleForkNoReuseTest.class)
public class TestSdcIpcToKafka {

  private static List<KafkaStream<byte[], byte[]>> kafkaStreams1;

  private static final int PARTITIONS = 3;
  private static final int REPLICATION_FACTOR = 2;
  private static final String TOPIC1 = "TestSdcIpcToKafka1";

  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  private static int randomPort;

  @BeforeClass
  public static void setUp() throws Exception {
    randomPort = NetworkUtils.getRandomPort();

    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(3);
    // create topic
    sdcKafkaTestUtil.createTopic(TOPIC1, PARTITIONS, REPLICATION_FACTOR);

    for (int i = 1; i <= 1 ; i++) {
      for (int j = 0; j < PARTITIONS; j++) {
        TestUtils.waitUntilMetadataIsPropagated(
            scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
            "TestSdcIpcToKafka" + String.valueOf(i), j, 5000);
      }
    }
    kafkaStreams1 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC1, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
  }

  @Test
  public void testWrite() throws Exception {
    // setup TLS
    String hostname = SSLTestUtils.getHostname();
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    File keyStore = new File(testDir, "keystore.jks");
    Assert.assertTrue(testDir.mkdirs());
    final File trustStore = new File(testDir, "truststore.jks");
    KeyPair keyPair = SSLTestUtils.generateKeyPair();
    Certificate cert = SSLTestUtils.generateCertificate("CN=" + hostname, keyPair, 30);
    SSLTestUtils.createKeyStore(keyStore.toString(), "keystore", "web", keyPair.getPrivate(), cert);
    SSLTestUtils.createTrustStore(trustStore.toString(), "truststore", "web", cert);

    // Configure stage
    final RpcConfigs configs = new RpcConfigs();
    configs.appId = "test";
    configs.maxConcurrentRequests = 10;
    configs.maxRpcRequestSize = 10000;
    configs.sslEnabled = true;
    configs.keyStoreFile = keyStore.toString();
    configs.keyStorePassword = "keystore";
    configs.port = randomPort;

    KafkaConfigBean kafkaConfigBean = new KafkaConfigBean();
    kafkaConfigBean.kafkaConfig = new KafkaTargetConfig();
    kafkaConfigBean.kafkaConfig.topic = TOPIC1;
    kafkaConfigBean.kafkaConfig.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();

    final SdcIpcToKafkaSource source = new SdcIpcToKafkaSource(configs, kafkaConfigBean, 1000);

    // create source runner
    final SourceRunner sourceRunner = new SourceRunner.Builder(SdcIpcToKafkaDSource.class, source)
      .addOutputLane("lane")
      .build();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // init
    try {
      sourceRunner.runInit();
      //valid init
      Future<Boolean> future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          HttpURLConnection conn = getConnection(Constants.IPC_PATH, configs.appId, sourceRunner.getContext(),
            SSLTestUtils.getHostname() + ":" + configs.port, true,
            trustStore.toString(), "truststore");
          conn.setRequestMethod("GET");
          conn.setDefaultUseCaches(false);
          conn.setDoOutput(false);
          return conn.getResponseCode() == HttpURLConnection.HTTP_OK &&
            Constants.X_SDC_PING_VALUE.equals(conn.getHeaderField(Constants.X_SDC_PING_HEADER));
        }
      });
      Assert.assertTrue(future.get(5, TimeUnit.SECONDS));

      // send request
      final List<Record> records = new ArrayList<>();
      future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          Record r1 = RecordCreator.create();
          r1.set(Field.create(true));
          Record r2 = RecordCreator.create();
          r2.set(Field.create(false));
          records.add(r1);
          records.add(r2);
          return sendRecords(
            configs.appId,
            sourceRunner.getContext(),
            SSLTestUtils.getHostname() + ":" + configs.port,
            true,
            trustStore.toString(),
            "truststore",
            true,
            records
          );
        }
      });

      // run produce
      StageRunner.Output output = sourceRunner.runProduce(null, 10);
      // Make sure there s no output in output lane
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      Assert.assertTrue(sourceRunner.getErrorRecords().isEmpty());
      Assert.assertTrue(sourceRunner.getErrors().isEmpty());

      Assert.assertTrue(future.get(30, TimeUnit.SECONDS));

      // check if kafka has received a message
      List<byte[]> messages = new ArrayList<>();
      Assert.assertTrue(kafkaStreams1.size() == PARTITIONS);
      for (KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams1) {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        try {
          while (it.hasNext()) {
            messages.add(it.next().message());
          }
        } catch (kafka.consumer.ConsumerTimeoutException e) {
          //no-op
        }
      }

      // there just 1 message containing 2 records
      Assert.assertEquals(1, messages.size());

      // parse message to get 2 records
      byte[] bytes = messages.get(0);
      RecordReader reader = ((ContextExtensions) sourceRunner.getContext()).createRecordReader(
        new ByteArrayInputStream(bytes), 0, 1000
      );
      List<Record> result = new ArrayList<>();
      Record record = reader.readRecord();
      while (record != null) {
        result.add(record);
        record = reader.readRecord();
      }

      Assert.assertEquals(2, result.size());
      Assert.assertEquals(true, result.get(0).get().getValueAsBoolean());
      Assert.assertEquals(false, result.get(1).get().getValueAsBoolean());

    } finally {
      sourceRunner.runDestroy();
    }
  }

  private boolean sendRecords(String appId, Stage.Context context, String hostPort, boolean ssl, String trustStoreFile,
                              String trustStorePassword, boolean compressed,  List<Record> records)
    throws Exception {
    try {
      ContextExtensions ext = (ContextExtensions) context;
      HttpURLConnection conn = getConnection(Constants.IPC_PATH, appId, context, hostPort, ssl, trustStoreFile,
        trustStorePassword);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.CONTENT_TYPE_HEADER, Constants.APPLICATION_BINARY);//X_SDC_JSON1_FRAGMENTABLE_HEADER
      conn.setRequestProperty(Constants.X_SDC_JSON1_FRAGMENTABLE_HEADER, "true");
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

  static final HostnameVerifier ACCEPT_ALL_HOSTNAME_VERIFIER = new HostnameVerifier() {
    @Override
    public boolean verify(String s, SSLSession sslSession) {
      return true;
    }
  };

  private HttpURLConnection getConnection(
      String path,
      String appId,
      Stage.Context context,
      String hostPort,
      boolean ssl,
      String trustStoreFile,
      String trustStorePassword
  ) throws Exception {

    boolean hostVerification = false;

    String scheme = (ssl) ? "https://" : "http://";
    URL url = new URL(scheme + hostPort.trim()  + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(1000);
    conn.setReadTimeout(60000);
    if (ssl) {
      HttpsURLConnection sslConn = (HttpsURLConnection) conn;
      sslConn.setSSLSocketFactory(createSSLSocketFactory(context, trustStoreFile, trustStorePassword));
      if (!hostVerification) {
        sslConn.setHostnameVerifier(ACCEPT_ALL_HOSTNAME_VERIFIER);
      }
    }
    conn.setRequestProperty(Constants.X_SDC_APPLICATION_ID_HEADER, appId);
    return conn;
  }

  private SSLSocketFactory createSSLSocketFactory(
      Stage.Context context,
      String trustStoreFile,
      String trustStorePassword
  ) throws Exception {
    SSLSocketFactory sslSocketFactory;
    if (trustStoreFile.isEmpty()) {
      sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    } else {
      KeyStore keystore = KeyStore.getInstance("jks");
      try (InputStream is = new FileInputStream(new File(context.getResourcesDirectory(), trustStoreFile))) {
        keystore.load(is, trustStorePassword.toCharArray());
      }

      KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(Constants.SSL_CERTIFICATE);
      keyMgrFactory.init(keystore, trustStorePassword.toCharArray());
      KeyManager[] keyManagers = keyMgrFactory.getKeyManagers();

      TrustManager[] trustManagers = new TrustManager[1];
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(Constants.SSL_CERTIFICATE);
      trustManagerFactory.init(keystore);
      for (TrustManager trustManager1 : trustManagerFactory.getTrustManagers()) {
        if (trustManager1 instanceof X509TrustManager) {
          trustManagers[0] = trustManager1;
          break;
        }
      }
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManagers, trustManagers, null);
      sslContext.getDefaultSSLParameters().setProtocols(Constants.SSL_ENABLED_PROTOCOLS);
      sslSocketFactory = sslContext.getSocketFactory();
    }
    return sslSocketFactory;
  }
}
