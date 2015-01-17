/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.json.StreamingJsonParser;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import kafka.javaapi.producer.Producer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestHighLevelKafkaSource {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static int port;
  private static String zkConnect;

  private static Producer<String, String> producer;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int SINGLE_PARTITION = 1;
  private static final int MULTIPLE_PARTITIONS = 5;
  private static final int REPLICATION_FACTOR = 1;
  private static final String CONSUMER_GROUP = "SDC";
  private static final int TIME_OUT = 2000;

  @BeforeClass
  public static void setUp() {
    //Init zookeeper
    zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(BROKER_ID, port);
    List<KafkaServer> servers = new ArrayList<>();
    kafkaServer = TestUtils.createServer(new KafkaConfig(props), new MockTime());
    servers.add(kafkaServer);

    producer = KafkaTestUtil.createProducer(HOST, port);
  }

  @AfterClass
  public static void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }

  @Test
  public void testProduceStringRecords() throws StageException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceStringRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceStringRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.LOG));

    SourceRunner sourceRunner = new SourceRunner.Builder(HighLevelKafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 5);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(5, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get().getValueAsString());
      Assert.assertTrue(!records.get(i).get().getValueAsString().isEmpty());
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.LOG), records.get(i).get().getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceStringRecordsMultiplePartitions() throws StageException {

    CountDownLatch startProducing = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceStringRecordsMultiplePartitions",
      MULTIPLE_PARTITIONS, REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceStringRecordsMultiplePartitions",
      MULTIPLE_PARTITIONS, producer, startProducing, DataType.LOG));

    SourceRunner sourceRunner = new SourceRunner.Builder(HighLevelKafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceStringRecordsMultiplePartitions")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();

    startProducing.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get().getValueAsString());
      Assert.assertTrue(!records.get(i).get().getValueAsString().isEmpty());
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.LOG), records.get(i).get().getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecords() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceJsonRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceJsonRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON));

    SourceRunner sourceRunner = new SourceRunner.Builder(HighLevelKafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceJsonRecords")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("payloadType", PayloadType.JSON)
      .addConfiguration("jsonContent", StreamingJsonParser.Mode.MULTIPLE_OBJECTS)
      .addConfiguration("maxJsonObjectLen", 4096)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    JsonFieldCreator jsonFieldCreator = new JsonFieldCreator(StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 4096);
    for(int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get().getValueAsMap());
      Assert.assertTrue(!records.get(i).get().getValueAsMap().isEmpty());
      Assert.assertEquals(
        jsonFieldCreator.createField(KafkaTestUtil.generateTestData(DataType.JSON).getBytes()).getValueAsMap(),
        records.get(i).get().getValueAsMap());
    }
    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceCsvRecords() throws StageException, IOException {
    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceCsvRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceCsvRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.CSV));

    SourceRunner sourceRunner = new SourceRunner.Builder(HighLevelKafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceCsvRecords")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("payloadType", PayloadType.CSV)
      .addConfiguration("csvFileFormat", CsvFileMode.CSV)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    CsvFieldCreator csvFieldCreator = new CsvFieldCreator(CsvFileMode.CSV);
    for (int i = 0; i < records.size(); i++) {
      Assert.assertNotNull(records.get(i).get().getValueAsMap());
      Assert.assertTrue(!records.get(i).get().getValueAsMap().isEmpty());
      Assert.assertEquals(
        csvFieldCreator.createField(KafkaTestUtil.generateTestData(DataType.CSV).getBytes()).getValueAsMap(),
        records.get(i).get().getValueAsMap());
    }
    sourceRunner.runDestroy();
  }
}
