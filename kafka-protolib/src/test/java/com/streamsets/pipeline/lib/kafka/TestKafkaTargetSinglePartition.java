/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import com.streamsets.pipeline.lib.util.JsonUtil;
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
    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", ProducerPayloadType.TEXT)
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

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", ProducerPayloadType.TEXT)
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

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", ProducerPayloadType.TEXT)
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
    Assert.assertEquals(18, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(logRecords.get(i).get().getValueAsMap().get("name").getValueAsString(), messages.get(i));
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON2() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", ProducerPayloadType.TEXT)
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
    Assert.assertEquals(18, messages.size());
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

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("payloadType", ProducerPayloadType.TEXT)
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

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", ProducerPayloadType.JSON)
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

    Assert.assertEquals(18, messages.size());
    for(int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(JsonUtil.jsonRecordToString(logRecords.get(i)), messages.get(i));
    }
  }

  @Test
  public void testWriteCsvRecords() throws InterruptedException, StageException, IOException {

    //Test CSV is - "2010,NLDS1,PHI,NL,CIN,NL,3,0,0"
    KafkaTarget.FieldPathToNameMappingConfig yearMapping =
      new KafkaTarget.FieldPathToNameMappingConfig();
    yearMapping.fieldPath = "/values[0]";
    yearMapping.columnName = "Year";

    KafkaTarget.FieldPathToNameMappingConfig cityMapping =
      new KafkaTarget.FieldPathToNameMappingConfig();
    cityMapping.fieldPath = "/values[2]";
    cityMapping.columnName = "City1";

    KafkaTarget.FieldPathToNameMappingConfig city2Mapping =
      new KafkaTarget.FieldPathToNameMappingConfig();
    city2Mapping.fieldPath = "/values[3]";
    city2Mapping.columnName = "City2";

    KafkaTarget.FieldPathToNameMappingConfig nonExistingmapping =
      new KafkaTarget.FieldPathToNameMappingConfig();
    nonExistingmapping.fieldPath = "/values[20]";
    nonExistingmapping.columnName = "NonExistingCity";

    KafkaTarget.FieldPathToNameMappingConfig city3Mapping =
      new KafkaTarget.FieldPathToNameMappingConfig();
    city3Mapping.fieldPath = "/values[4]";
    city3Mapping.columnName = "City3";

    KafkaTarget kafkaTarget = new KafkaTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(kafkaTarget)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", ProducerPayloadType.CSV)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("csvFileFormat", CsvFileMode.CSV)
      .addConfiguration("fieldPathToNameMappingConfigList", ImmutableList.of(yearMapping, cityMapping,city2Mapping, nonExistingmapping, city3Mapping))
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

    RecordToString recordToString = new CsvRecordToString(CSVFormat.DEFAULT);
    Map<String, String> fieldPathToName = new LinkedHashMap<>();
    fieldPathToName.put("/values[0]", "Year");
    fieldPathToName.put("/values[2]", "City1");
    fieldPathToName.put("/values[3]", "City2");
    fieldPathToName.put("/values[20]", "NonExistingCity");
    fieldPathToName.put("/values[4]", "City3");
    recordToString.setFieldPathToNameMapping(fieldPathToName);

    for (int i = 0; i < logRecords.size(); i++) {
      Assert.assertEquals(recordToString.toString(logRecords.get(i)), messages.get(i));
    }
  }
}