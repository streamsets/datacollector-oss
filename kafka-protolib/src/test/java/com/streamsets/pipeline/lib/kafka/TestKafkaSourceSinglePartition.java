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
import com.streamsets.pipeline.lib.util.CsvUtil;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestKafkaSourceSinglePartition {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static int port;

  private static Producer<String, String> producer;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "test";
  private static final int TIME_OUT = 2000;

  @BeforeClass
  public static void setUp() {
    //Init zookeeper
    String zkConnect = TestZKUtils.zookeeperConnect();
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

    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceStringRecords", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testProduceStringRecords",
      String.valueOf(0));
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceStringRecords")
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("fromBeginning", true)
      .addConfiguration("maxBatchSize", 64000)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("minBatchSize", 100)
      .addConfiguration("consumerPayloadType", ConsumerPayloadType.LOG)
      .build();

    sourceRunner.runInit();
    StageRunner.Output output = sourceRunner.runProduce(null, 5);

    String newOffset = output.getNewOffset();
    Assert.assertEquals("5", newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(5, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertEquals(data.get(i).message(), records.get(i).get().getValueAsString());
    }

    output = sourceRunner.runProduce(newOffset, 5);
    records = output.getRecords().get("lane");
    Assert.assertEquals(4, records.size());
    for(int i = 5; i < records.size(); i++) {
      Assert.assertEquals(data.get(i).message(), records.get(i).get().getValueAsString());
    }
    sourceRunner.runDestroy();

  }

  @Test
  public void testProduceStringRecordsFromEnd() throws StageException {

    CountDownLatch startProducing = new CountDownLatch(1);

    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceStringRecordsFromEnd", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable("testProduceStringRecordsFromEnd", 1, producer,
      startProducing, DataType.LOG));

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceStringRecordsFromEnd")
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("fromBeginning", false)
      .addConfiguration("maxBatchSize", 64000)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("minBatchSize", 100)
      .addConfiguration("consumerPayloadType", ConsumerPayloadType.LOG)
      .build();

    sourceRunner.runInit();
    StageRunner.Output output = sourceRunner.runProduce(null, 5);
    String newOffset = output.getNewOffset();
    Assert.assertEquals("0", newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(0, records.size());

    startProducing.countDown();
    output = sourceRunner.runProduce(null, 5);
    executorService.shutdown();

    newOffset = output.getNewOffset();
    Assert.assertTrue(newOffset != null && !newOffset.isEmpty());
    records = output.getRecords().get("lane");
    Assert.assertFalse(records.isEmpty());

    for(Record r : records) {
      Assert.assertTrue(r.get().getValueAsString() != null);
      Assert.assertTrue(!r.get().getValueAsString().isEmpty());
    }

    sourceRunner.runDestroy();

  }

  @Test
  public void testProduceJsonRecords() throws StageException, IOException {

    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceJsonRecords", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceJsonMessages("testProduceJsonRecords",
      String.valueOf(0));
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceJsonRecords")
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("fromBeginning", true)
      .addConfiguration("maxBatchSize", 64000)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("minBatchSize", 100)
      .addConfiguration("jsonContent", StreamingJsonParser.Mode.MULTIPLE_OBJECTS)
      .addConfiguration("maxJsonObjectLen", 4096)
      .addConfiguration("consumerPayloadType", ConsumerPayloadType.JSON)
      .build();

    sourceRunner.runInit();
    StageRunner.Output output = sourceRunner.runProduce(null, 10);

    String newOffset = output.getNewOffset();
    Assert.assertEquals("10", newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(10, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertEquals(data.get(i).message(), JsonUtil.jsonRecordToString(records.get(i)));
    }

    output = sourceRunner.runProduce(newOffset, 10);
    records = output.getRecords().get("lane");
    Assert.assertEquals(8, records.size());
    for(int i = 10; i < records.size(); i++) {
      Assert.assertEquals(data.get(i).message(), JsonUtil.jsonRecordToString(records.get(i)));
    }
    sourceRunner.runDestroy();

  }

  @Test
  public void testProduceCsvRecords() throws StageException, IOException {
    KafkaTestUtil.createTopic(zkClient, ImmutableList.of(kafkaServer), "testProduceCsvRecords", PARTITIONS,
      REPLICATION_FACTOR, TIME_OUT);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceCsvMessages("testProduceCsvRecords",
      String.valueOf(0), CSVFormat.DEFAULT);
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testProduceCsvRecords")
      .addConfiguration("partition", 0)
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
      .addConfiguration("fromBeginning", true)
      .addConfiguration("maxBatchSize", 64000)
      .addConfiguration("maxWaitTime", 5000)
      .addConfiguration("minBatchSize", 100)
      .addConfiguration("csvFileFormat", CsvFileMode.CSV)
      .addConfiguration("consumerPayloadType", ConsumerPayloadType.CSV)
      .build();

    sourceRunner.runInit();
    StageRunner.Output output = sourceRunner.runProduce(null, 15);

    String newOffset = output.getNewOffset();
    Assert.assertEquals("15", newOffset);
    List<Record> records = output.getRecords().get("lane");
    Assert.assertEquals(15, records.size());

    for(int i = 0; i < records.size(); i++) {
      Assert.assertEquals(data.get(i).message(), CsvUtil.csvRecordToString(records.get(i), CSVFormat.DEFAULT));
    }

    output = sourceRunner.runProduce(newOffset, 15);
    records = output.getRecords().get("lane");
    Assert.assertEquals(13, records.size());
    for(int i = 15; i < records.size(); i++) {
      Assert.assertEquals(data.get(i).message(), CsvUtil.csvRecordToString(records.get(i), CSVFormat.DEFAULT));
    }
    sourceRunner.runDestroy();

  }
}
