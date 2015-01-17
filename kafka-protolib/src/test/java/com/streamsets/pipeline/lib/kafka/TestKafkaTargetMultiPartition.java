/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestKafkaTargetMultiPartition {

  private static List<KafkaServer> kafkaServers;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;
  private static int port;

  private static final String HOST = "localhost";
  private static final int BROKER_1_ID = 0;
  private static final int BROKER_2_ID = 1;
  private static final int BROKER_3_ID = 2;
  private static final int PARTITIONS = 3;
  private static final int REPLICATION_FACTOR = 2;
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
    kafkaServers = new ArrayList<>(3);
    Properties props1 = TestUtils.createBrokerConfig(BROKER_1_ID, port);
    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props1), new MockTime()));
    Properties props2 = TestUtils.createBrokerConfig(BROKER_2_ID, TestUtils.choosePort());
    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props2), new MockTime()));
    Properties props3 = TestUtils.createBrokerConfig(BROKER_3_ID, TestUtils.choosePort());
    kafkaServers.add(TestUtils.createServer(new KafkaConfig(props3), new MockTime()));
    // create topic
    AdminUtils.createTopic(zkClient, TOPIC, PARTITIONS, REPLICATION_FACTOR, new Properties());
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(kafkaServers), TOPIC, 0, TIME_OUT);

    kafkaStreams = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    for(KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
    }
    zkClient.close();
    zkServer.shutdown();
  }

  @Test
  public void testWriteStringRecordsRoundRobin() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "-1")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.ROUND_ROBIN)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
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
    Assert.assertTrue(kafkaStreams.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
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
        Assert.assertTrue(records.contains(message));
      }
      messages.clear();
    }
  }

  @Test
  public void testWriteStringRecordsRandom() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "-1")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.RANDOM)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
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
    Assert.assertTrue(kafkaStreams.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }

      for(String message : messages) {
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

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      //record has a map which contains an integer field with key "partitionKey",
      //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "record:value(\"/\") % 3")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
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
    Assert.assertTrue(kafkaStreams.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
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
        Assert.assertTrue(records.contains(message));
      }
      messages.clear();
    }
  }

  @Test
  public void testInvalidPartitionExpression() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "value(\"/\") % 3")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    try {
      targetRunner.runInit();
      Assert.fail("Expected StageException as the partition expression is not valid");
    } catch (StageException e) {
      Assert.assertEquals(KafkaStageLibError.LIB_0357, e.getErrorCode());
    }
  }

  @Test
  public void testPartitionExpressionEvaluationError() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "record:value(\"/\") % 3")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

  }

  @Test
  public void testPartitionNumberOutOfRange() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "13")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

  }

  @Test
  public void testInvalidPartition() throws InterruptedException, StageException {

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "record:value(\"/\")")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", PayloadType.LOG)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

  }
}
