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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.lib.http.HttpConstants;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.origin.tokafka.HttpServerToKafkaSource;
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

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Category(SingleForkNoReuseTest.class)
public class TestSdcIpcToKafka {
  // copy of com.streamsets.pipeline.stage.destination.sdcipc.Constants
  private static final String APPLICATION_BINARY = "application/binary";
  private static final String X_SDC_JSON1_FRAGMENTABLE_HEADER = "X-SDC-JSON1-FRAGMENTABLE";

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
    // Configure stage
    final SdcIpcConfigs configs = new SdcIpcConfigs();
    configs.appId = () -> "test";
    configs.maxConcurrentRequests = 10;
    configs.maxRpcRequestSize = 10000;
    configs.tlsConfigBean.tlsEnabled = false;
    configs.port = randomPort;

    KafkaTargetConfig kafkaConfigBean = new KafkaTargetConfig();
    kafkaConfigBean.topic = TOPIC1;
    kafkaConfigBean.metadataBrokerList = sdcKafkaTestUtil.getMetadataBrokerURI();

    final HttpServerToKafkaSource source = new SdcIpcToKafkaSource(configs, kafkaConfigBean, 1000);

    // create source runner
    final SourceRunner sourceRunner = new SourceRunner.Builder(SdcIpcToKafkaDSource.class, source)
      .addOutputLane("lane")
      .build();

    // init
    try {
      sourceRunner.runInit();

      //ping
      URL url = new URL("http://localhost:" + randomPort + SdcIpcToKafkaSource.IPC_PATH);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "test");
      conn.setRequestMethod("GET");
      conn.setDefaultUseCaches(false);
      conn.setDoOutput(false);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      Assert.assertEquals(HttpConstants.X_SDC_PING_VALUE, conn.getHeaderField(HttpConstants.X_SDC_PING_HEADER));

      // send data
      url = new URL("http://localhost:" + randomPort + SdcIpcToKafkaSource.IPC_PATH);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(HttpConstants.X_SDC_APPLICATION_ID_HEADER, "test");
      conn.setRequestMethod("POST");
      conn.setDefaultUseCaches(false);
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", APPLICATION_BINARY);
      conn.setRequestProperty(X_SDC_JSON1_FRAGMENTABLE_HEADER, "true");
      conn.setRequestProperty(HttpConstants.X_SDC_COMPRESSION_HEADER, HttpConstants.SNAPPY_COMPRESSION);
      OutputStream os = conn.getOutputStream();
      os = new SnappyFramedOutputStream(os);
      ContextExtensions ext = (ContextExtensions) sourceRunner.getContext();
      RecordWriter writer = ext.createRecordWriter(os);
      final List<Record> records = new ArrayList<>();
      Record r1 = RecordCreator.create();
      r1.set(Field.create(true));
      Record r2 = RecordCreator.create();
      r2.set(Field.create(false));
      records.add(r1);
      records.add(r2);
      for (Record record : records) {
        writer.write(record);
      }
      writer.close();
      Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

      // run produce
      StageRunner.Output output = sourceRunner.runProduce(null, 10);
      // Make sure there s no output in output lane
      Assert.assertEquals(0, output.getRecords().get("lane").size());
      Assert.assertTrue(sourceRunner.getErrorRecords().isEmpty());
      Assert.assertTrue(sourceRunner.getErrors().isEmpty());

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

}
