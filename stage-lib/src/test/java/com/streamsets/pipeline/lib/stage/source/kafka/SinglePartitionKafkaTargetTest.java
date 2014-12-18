/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.stage.source.util.CsvUtil;
import com.streamsets.pipeline.lib.stage.source.util.JsonUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import kafka.admin.AdminUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.csv.CSVFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SinglePartitionKafkaTargetTest {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;
  private static int port;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "test";
  private static final int TIME_OUT = 1000;

  @BeforeClass
  public static void setUp() {
    //Init zookeeper
    String zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(BROKER_ID, port);
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    // create topic
    AdminUtils.createTopic(zkClient, TOPIC, PARTITIONS, REPLICATION_FACTOR, new Properties());
    List<KafkaServer> servers = new ArrayList<>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC, 0, TIME_OUT);

    kafkaStreams = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }

  @Test
  public void testWriteNoRecords() throws InterruptedException, StageException {
    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("payloadType", PayloadType.STRING)
      .addConfiguration("partitionStrategy", PartitionStrategy.FIXED)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createEmptyLogRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(0, messages.size());
  }

  @Test
  public void testWriteStringRecords() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("payloadType", PayloadType.STRING)
      .addConfiguration("partitionStrategy", PartitionStrategy.FIXED)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(9, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsString(), messages.get(i));
    }
  }

  @Test
  public void testWriteJsonRecords() throws InterruptedException, StageException, IOException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("payloadType", PayloadType.JSON)
      .addConfiguration("partitionStrategy", PartitionStrategy.FIXED)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(18, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(JsonUtil.jsonRecordToString(logRecords.get(i)), messages.get(i));
    }
  }

  @Test
  public void testWriteCsvRecords() throws InterruptedException, StageException, IOException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("payloadType", PayloadType.CSV)
      .addConfiguration("partitionStrategy", PartitionStrategy.FIXED)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createCsvRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(28, messages.size());
    for (int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(CsvUtil.csvRecordToString(logRecords.get(i), CSVFormat.DEFAULT), messages.get(i));
    }
  }
}