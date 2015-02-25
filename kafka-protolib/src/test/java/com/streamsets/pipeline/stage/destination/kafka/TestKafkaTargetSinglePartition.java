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
import com.streamsets.pipeline.api.ext.JsonRecordReader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
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
import org.apache.commons.csv.CSVFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestKafkaTargetSinglePartition {

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
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", DataFormat.TEXT)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .addConfiguration("fieldPath", "/")
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

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", DataFormat.TEXT)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .addConfiguration("fieldPath", "/")
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
  public void testWriteStringRecordsFromJSON() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", DataFormat.TEXT)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .addConfiguration("fieldPath", "/name")
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
        String message = new String(it.next().message());
        messages.add(message);
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(20, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsMap().get("name").getValueAsString(), messages.get(i));
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON2() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", DataFormat.TEXT)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .addConfiguration("fieldPath", "/lastStatusChange") //this is number field, should be converted to string
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
        String message = new String(it.next().message());
        messages.add(message);
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(20, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsMap().get("lastStatusChange").getValueAsString(),
        messages.get(i));
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON3() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", DataFormat.TEXT)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .addConfiguration("fieldPath", "/") //this is map field, should not be converted to string
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    //All records must be sent to error
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    //Double check that there are no messages in kafka target topic
    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams.get(0).iterator();
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
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", DataFormat.SDC_JSON)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
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

    Assert.assertEquals(20, messages.size());

    ContextExtensions ctx = (ContextExtensions) ContextInfoCreator.createTargetContext("", false, OnRecordError.TO_ERROR);
    for(int i = 0; i < logRecords.size(); i++) {
      JsonRecordReader rr = ctx.createJsonRecordReader(new StringReader(messages.get(i)), 0, Integer.MAX_VALUE);
      Assert.assertEquals(logRecords.get(i), rr.readRecord());
      rr.close();
    }
  }

  @Test
  public void testWriteCsvRecords() throws InterruptedException, StageException, IOException {

    //Test DELIMITED is - "2010,NLDS1,PHI,NL,CIN,NL,3,0,0"
    List<String> fieldPaths = new ArrayList<>();
    fieldPaths.add("/values[0]");
    fieldPaths.add("/values[2]");
    fieldPaths.add("/values[3]");
    fieldPaths.add("/values[20]");
    fieldPaths.add("/values[4]");

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", DataFormat.DELIMITED)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", CsvMode.CSV)
      .addConfiguration("fieldPaths", fieldPaths)
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

    RecordToString recordToString = new CsvRecordToString(CSVFormat.DEFAULT, false
    );
    Map<String, String> fieldPathToName = new LinkedHashMap<>();
    fieldPathToName.put("/values[0]", null);
    fieldPathToName.put("/values[2]", null);
    fieldPathToName.put("/values[3]", null);
    fieldPathToName.put("/values[20]", null);
    fieldPathToName.put("/values[4]", null);
    recordToString.setFieldPathToNameMapping(fieldPathToName);

    for (int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(recordToString.toString(logRecords.get(i)), messages.get(i));
    }
  }
}