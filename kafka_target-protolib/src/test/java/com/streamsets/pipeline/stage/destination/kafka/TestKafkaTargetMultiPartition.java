/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.KafkaTestUtil;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TestKafkaTargetMultiPartition {

  private static List<KafkaServer> kafkaServers;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams1;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams2;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams3;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams4;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams5;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams6;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams7;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams8;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams9;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams10;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams11;
  private static int port;

  private static final String HOST = "localhost";
  private static final int BROKER_1_ID = 0;
  private static final int BROKER_2_ID = 1;
  private static final int BROKER_3_ID = 2;
  private static final int PARTITIONS = 3;
  private static final int REPLICATION_FACTOR = 2;
  private static final String TOPIC1 = "test1";
  private static final String TOPIC2 = "test2";
  private static final String TOPIC3 = "test3";
  private static final String TOPIC4 = "test4";
  private static final String TOPIC5 = "test5";
  private static final String TOPIC6 = "test6";
  private static final String TOPIC7 = "test7";
  private static final String TOPIC8 = "test8";
  private static final String TOPIC9 = "test9";
  private static final String TOPIC10 = "test10";
  private static final String TOPIC11 = "test11";
  private static final int TIME_OUT = 5000;

  private static String originalTmpDir;

  @BeforeClass
  public static void setUp() {
    //Init zookeeper
    originalTmpDir = System.getProperty("java.io.tmpdir");
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty("java.io.tmpdir", testDir.getAbsolutePath());

    String zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    // setup Broker
    port = TestUtils.choosePort();
    kafkaServers = new ArrayList<>(3);
    Properties props1 = TestUtils.createBrokerConfig(BROKER_1_ID, port);
    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props1), new MockTime()));
    Properties props2 = TestUtils.createBrokerConfig(BROKER_2_ID, TestUtils.choosePort());
    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props2), new MockTime()));
    Properties props3 = TestUtils.createBrokerConfig(BROKER_3_ID, TestUtils.choosePort());
    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props3), new MockTime()));
    // create topic
    AdminUtils.createTopic(zkClient, TOPIC1, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC2, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC3, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC4, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC5, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC6, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC7, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC8, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC9, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC10, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC11, PARTITIONS, REPLICATION_FACTOR, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC1, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC2, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC3, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC4, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC5, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC6, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC7, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC8, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC9, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC10, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC11, 0, TIME_OUT);

    kafkaStreams1 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC1, PARTITIONS);
    kafkaStreams2 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC2, PARTITIONS);
    kafkaStreams3 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC3, PARTITIONS);
    kafkaStreams4 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC4, PARTITIONS);
    kafkaStreams5 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC5, PARTITIONS);
    kafkaStreams6 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC6, PARTITIONS);
    kafkaStreams7 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC7, PARTITIONS);
    kafkaStreams8 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC8, PARTITIONS);
    kafkaStreams9 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC9, PARTITIONS);
    kafkaStreams10 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC10, PARTITIONS);
    kafkaStreams11 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC11, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    for(KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
    }
    zkClient.close();
    zkServer.shutdown();
    System.setProperty("java.io.tmpdir", originalTmpDir);
  }

  @Test
  public void testWriteStringRecordsRoundRobin() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC1)
      .addConfiguration("partition", "-1")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.ROUND_ROBIN)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams1.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams1) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(3, messages.size());
      for(String message : messages) {
        Assert.assertTrue(records.contains(message.trim()));
      }
      messages.clear();
    }
  }

  @Test
  public void testWriteStringRecordsRandom() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC2)
      .addConfiguration("partition", "-1")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.RANDOM)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams2.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams2) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }

      for(String message : messages) {
        message = message.trim();
        Assert.assertTrue(records.contains(message));
        records.remove(message);
      }
      messages.clear();
    }

    //make sure we have seen all the records.
    Assert.assertEquals(0, records.size());
  }

  @Test
  public void testExpressionPartitioner() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC3)
      //record has a map which contains an integer field with key "partitionKey",
      //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/') % 3}")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createIntegerRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams3.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams3) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(3, messages.size());
      for(String message : messages) {
        Assert.assertTrue(records.contains(message.trim()));
      }
      messages.clear();
    }
  }

  @Test
  public void testInvalidPartitionExpression() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", TOPIC4)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${value('/') % 3}")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    List<Stage.ConfigIssue> configIssues = targetRunner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());

  }

  @Test
  public void testPartitionExpressionEvaluationError() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", TOPIC5)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/') % 3}")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();

  }

  @Test
  public void testPartitionNumberOutOfRange() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", TOPIC6)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "13")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }

  @Test
  public void testInvalidPartition() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", TOPIC7)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/')}")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();

  }

  @Test
  public void testExpressionPartitionerSingleMessage() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC8)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/') % 3}")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", true)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createIntegerRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams8.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams8) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(1, messages.size());
      messages.clear();
    }
  }

  @Test
  public void testMultiTopicMultiPartitionSingleMessage() throws InterruptedException, StageException, IOException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", null)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/partition') % 3}")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", true)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/topic")
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", true)
      .addConfiguration("topicExpression", "${record:value('/topic')}")
      .addConfiguration("topicWhiteList", "*")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecordsWithTopicPartitionField(ImmutableList.of(TOPIC9, TOPIC10, TOPIC11),
      PARTITIONS);
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get("/topic").getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams9.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams9) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(1, messages.size());
      messages.clear();
    }
    Assert.assertTrue(kafkaStreams10.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams10) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(1, messages.size());
      messages.clear();
    }
    Assert.assertTrue(kafkaStreams11.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams11) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(1, messages.size());
      messages.clear();
    }
  }

}
