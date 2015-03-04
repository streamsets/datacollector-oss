/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.DataType;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.lib.ProducerRunnable;
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

public class TestKafkaSource {

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
      producer, startLatch, DataType.LOG, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecord", false)
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
      Assert.assertNotNull(records.get(i).get("/text"));
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.LOG, null),
                          records.get(i).get("/text").getValueAsString());
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
      MULTIPLE_PARTITIONS, producer, startProducing, DataType.LOG, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceStringRecordsMultiplePartitions")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecord", false)
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
      Assert.assertNotNull(records.get(i).get("/text").getValueAsString());
      Assert.assertTrue(!records.get(i).get("/text").getValueAsString().isEmpty());
      Assert.assertEquals(KafkaTestUtil.generateTestData(DataType.LOG, null), records.get(i).get("/text").getValueAsString());
    }

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsSingleRecord() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer),
      "testProduceJsonRecordsMultipleObjectsSingleRecord", SINGLE_PARTITION, REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceJsonRecordsMultipleObjectsSingleRecord", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON, StreamingJsonParser.Mode.MULTIPLE_OBJECTS));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceJsonRecordsMultipleObjectsSingleRecord")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("jsonContent", JsonMode.MULTIPLE_OBJECTS)
      .addConfiguration("produceSingleRecord", true)
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

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsMultipleObjectsMultipleRecord() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceJsonRecordsMultipleObjectsMultipleRecord", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceJsonRecordsMultipleObjectsMultipleRecord", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON, StreamingJsonParser.Mode.MULTIPLE_OBJECTS));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceJsonRecordsMultipleObjectsMultipleRecord")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("jsonContent", JsonMode.MULTIPLE_OBJECTS)
      .addConfiguration("produceSingleRecord", false)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 12);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(12, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceJsonRecordsArrayObjects() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceJsonRecordsArrayObjects", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceJsonRecordsArrayObjects", SINGLE_PARTITION,
      producer, startLatch, DataType.JSON, StreamingJsonParser.Mode.ARRAY_OBJECTS));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceJsonRecordsArrayObjects")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.JSON)
      .addConfiguration("jsonContent", JsonMode.ARRAY_OBJECTS)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecord", true)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceXmlRecords() throws StageException, IOException {

    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceXmlRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceXmlRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.XML, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceXmlRecords")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.XML)
      .addConfiguration("jsonContent", null)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecord", true)
      .addConfiguration("xmlRecordElement", "")
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);

    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }

  @Test
  public void testProduceCsvRecords() throws StageException, IOException {
    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceCsvRecords", SINGLE_PARTITION,
      REPLICATION_FACTOR, TIME_OUT);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable( "testProduceCsvRecords", SINGLE_PARTITION,
      producer, startLatch, DataType.CSV, null));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceCsvRecords")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", zkConnect)
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("dataFormat", DataFormat.DELIMITED)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("kafkaConsumerConfigs", null)
      .addConfiguration("produceSingleRecord", true)
      .build();

    sourceRunner.runInit();

    startLatch.countDown();
    StageRunner.Output output = sourceRunner.runProduce(null, 9);
    executorService.shutdown();

    String newOffset = output.getNewOffset();
    Assert.assertNull(newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(9, records.size());

    sourceRunner.runDestroy();
  }
}
