/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestKafkaTargetSinglePartition {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams1;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams2;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams3;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams4;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams5;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams6;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams7;
  private static int port;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC1 = "test1";
  private static final String TOPIC2 = "test2";
  private static final String TOPIC3 = "test3";
  private static final String TOPIC4 = "test4";
  private static final String TOPIC5 = "test5";
  private static final String TOPIC6 = "test6";
  private static final String TOPIC7 = "test7";
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
    AdminUtils.createTopic(zkClient, TOPIC1, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC2, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC3, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC4, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC5, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC6, PARTITIONS, REPLICATION_FACTOR, new Properties());
    AdminUtils.createTopic(zkClient, TOPIC7, PARTITIONS, REPLICATION_FACTOR, new Properties());
    List<KafkaServer> servers = new ArrayList<>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC1, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC2, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC3, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC4, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC5, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC6, 0, TIME_OUT);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asBuffer(servers), TOPIC7, 0, TIME_OUT);

    kafkaStreams1 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC1, PARTITIONS);
    kafkaStreams2 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC2, PARTITIONS);
    kafkaStreams3 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC3, PARTITIONS);
    kafkaStreams4 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC4, PARTITIONS);
    kafkaStreams5 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC5, PARTITIONS);
    kafkaStreams6 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC6, PARTITIONS);
    kafkaStreams7 = KafkaTestUtil.createKafkaStream(zkServer.connectString(), TOPIC7, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }

  @Test
  public void testWriteNoRecords() throws InterruptedException, StageException {
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC1)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createEmptyLogRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams1.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams1.get(0).iterator();
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

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC2)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams2.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams2.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(9, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsString(), messages.get(i).trim());
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC3)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/name")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams3.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams3.get(0).iterator();
    try {
      while (it.hasNext()) {
        String message = new String(it.next().message());
        messages.add(message);
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(20, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsMap().get("name").getValueAsString(), messages.get(i).trim());
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON2() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC4)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/lastStatusChange") //this is number field, should be converted to string
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams4.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams4.get(0).iterator();
    try {
      while (it.hasNext()) {
        String message = new String(it.next().message());
        messages.add(message);
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(20, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsMap().get("lastStatusChange").getValueAsString(),
        messages.get(i).trim());
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON3() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", TOPIC5)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/") //this is map field, should not be converted to string
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    //All records must be sent to error
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    //Double check that there are no messages in kafka target topic
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams5.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams5.get(0).iterator();
    try {
      while (it.hasNext()) {
        String message = new String(it.next().message());
        messages.add(message);
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    //Nothing should be written to the target topic
    Assert.assertEquals(0, messages.size());

  }

  @Test
  public void testWriteJsonRecords() throws InterruptedException, StageException, IOException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC6)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.SDC_JSON)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<byte[]> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams6.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams6.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(it.next().message());
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(20, messages.size());

    ContextExtensions ctx = (ContextExtensions) ContextInfoCreator.createTargetContext("", false, OnRecordError.TO_ERROR);
    for(int i = 0; i < logRecords.size(); i++) {
      ByteArrayInputStream bais = new ByteArrayInputStream(messages.get(i));
      RecordReader rr = ctx.createRecordReader(bais, 0, Integer.MAX_VALUE);
      Assert.assertEquals(logRecords.get(i), rr.readRecord());
      rr.close();
    }
  }

  @Test
  public void testWriteCsvRecords() throws InterruptedException, StageException, IOException {

    //Test DELIMITED is - "2010,NLDS1,PHI,NL,CIN,NL,3,0,0"

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC7)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.DELIMITED)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("csvHeader", CsvHeader.NO_HEADER)
      .addConfiguration("csvReplaceNewLines", false)
      .addConfiguration("charset", "UTF-8")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createCsvRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams7.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams7.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(28, messages.size());

  }
}
