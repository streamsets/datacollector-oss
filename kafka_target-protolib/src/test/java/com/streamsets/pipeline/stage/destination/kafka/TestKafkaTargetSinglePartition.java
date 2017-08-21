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
package com.streamsets.pipeline.stage.destination.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.lib.util.SdcAvroTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.kafka.util.KafkaTargetUtil;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.TestUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
/**
 * Currently ignored due to issues with {@link #testTopicExpression4()} and {@link #testInvalidTopicWhiteList()}
 * when run using maven even with SingleForkNoReuseTest.
 */
@Category(SingleForkNoReuseTest.class)
public class TestKafkaTargetSinglePartition {

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
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams12;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams13;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams14;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams15;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams16;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams17;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams18;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams19;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams20;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams21;

  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC1 = "TestKafkaTargetSinglePartition1";
  private static final String TOPIC2 = "TestKafkaTargetSinglePartition2";
  private static final String TOPIC3 = "TestKafkaTargetSinglePartition3";
  private static final String TOPIC4 = "TestKafkaTargetSinglePartition4";
  private static final String TOPIC5 = "TestKafkaTargetSinglePartition5";
  private static final String TOPIC6 = "TestKafkaTargetSinglePartition6";
  private static final String TOPIC7 = "TestKafkaTargetSinglePartition7";
  private static final String TOPIC8 = "TestKafkaTargetSinglePartition8";
  private static final String TOPIC9 = "TestKafkaTargetSinglePartition9";
  private static final String TOPIC10 = "TestKafkaTargetSinglePartition10";
  private static final String TOPIC11 = "TestKafkaTargetSinglePartition11";
  private static final String TOPIC12 = "TestKafkaTargetSinglePartition12";
  private static final String TOPIC13 = "TestKafkaTargetSinglePartition13";
  private static final String TOPIC14 = "TestKafkaTargetSinglePartition14";
  private static final String TOPIC15 = "TestKafkaTargetSinglePartition15";
  private static final String TOPIC16 = "TestKafkaTargetSinglePartition16";
  private static final String TOPIC17 = "TestKafkaTargetSinglePartition17";
  private static final String TOPIC18 = "TestKafkaTargetSinglePartition18";
  private static final String TOPIC19 = "TestKafkaTargetSinglePartition19";
  private static final String TOPIC20 = "TestKafkaTargetSinglePartition20";
  private static final String TOPIC21 = "TestKafkaTargetSinglePartition21";

  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(1);
    // create topic
    sdcKafkaTestUtil.createTopic(TOPIC1, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC2, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC3, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC4, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC5, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC6, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC7, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC8, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC9, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC10, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC11, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC12, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC13, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC14, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC15, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC16, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC17, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC18, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC19, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC20, PARTITIONS, REPLICATION_FACTOR);
    sdcKafkaTestUtil.createTopic(TOPIC21, PARTITIONS, REPLICATION_FACTOR);

    for (int i = 1; i <= 21 ; i++) {
      TestUtils.waitUntilMetadataIsPropagated(
          scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
          "TestKafkaTargetSinglePartition" + String.valueOf(i), 0, 5000);
    }


    kafkaStreams1 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC1, PARTITIONS);
    kafkaStreams2 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC2, PARTITIONS);
    kafkaStreams3 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC3, PARTITIONS);
    kafkaStreams4 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC4, PARTITIONS);
    kafkaStreams5 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC5, PARTITIONS);
    kafkaStreams6 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC6, PARTITIONS);
    kafkaStreams7 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC7, PARTITIONS);
    kafkaStreams8 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC8, PARTITIONS);
    kafkaStreams9 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC9, PARTITIONS);
    kafkaStreams10 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC10, PARTITIONS);
    kafkaStreams11 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC11, PARTITIONS);
    kafkaStreams12 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC12, PARTITIONS);
    kafkaStreams13 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC13, PARTITIONS);
    kafkaStreams14 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC14, PARTITIONS);
    kafkaStreams15 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC15, PARTITIONS);
    kafkaStreams16 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC16, PARTITIONS);
    kafkaStreams17 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC17, PARTITIONS);
    kafkaStreams18 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC18, PARTITIONS);
    kafkaStreams19 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC19, PARTITIONS);
    kafkaStreams20 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC20, PARTITIONS);
    kafkaStreams21 = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC21, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
  }

  @Test
  public void testWriteNoRecords() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
      sdcKafkaTestUtil.getMetadataBrokerURI(),
      TOPIC1,
      "0",                               // partition
      sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
      false,                              // singleMessagePerBatch
      PartitionStrategy.ROUND_ROBIN,
      false,                              // runtimeTopicResolution
      null,                               // topicExpression
      null,                               // topic white list
      new KafkaTargetConfig(),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createEmptyLogRecords();
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

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC2,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.ROUND_ROBIN,
        false,                              // runtimeTopicResolution
        null,                               // topicExpression
        null,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
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

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/name";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC3,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.ROUND_ROBIN,
        false,                              // runtimeTopicResolution
        null,                               // topicExpression
        null,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecords();
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

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/lastStatusChange";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC4,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        false,                              // runtimeTopicResolution
        null,                               // topicExpression
        null,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecords();
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

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/"; //map, invalid
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC5,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        false,                              // runtimeTopicResolution
        null,                               // topicExpression
        null,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecords();
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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC6,
        "0",                               // partition
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        false,                              // runtimeTopicResolution
        null,                               // topicExpression
        null,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecords();
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
  public void testWriteCsvRecords() throws Exception {

    //Test DELIMITED is - "2010,NLDS1,PHI,NL,CIN,NL,3,0,0"

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.csvFileFormat = CsvMode.CSV;
    dataGeneratorFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    dataGeneratorFormatConfig.csvReplaceNewLines = false;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC7,
        "0",                               // partition
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        false,                              // runtimeTopicResolution
        null,                               // topicExpression
        null,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.DELIMITED,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    File f = new File(Resources.getResource("testKafkaTarget.csv").toURI());
    List<Record> logRecords = sdcKafkaTestUtil.createCsvRecords(f);
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

  @Test
  /**
   * Tests runtime topic resolution from record where topics resolved are part of the white list.
   * Tests for both 'single message per record' and 'single message per batch' options.
   */
  public void testTopicExpression1() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        true,                              // runtimeTopicResolution
        "${record:value('/topic')}",                               // topicExpression
        TOPIC8 + ", " + TOPIC9 + ", " + TOPIC10,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC8, TOPIC9, TOPIC10));
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams8.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams8.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams9.size() == 1);
    it = kafkaStreams9.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams10.size() == 1);
    it = kafkaStreams10.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    //single message per batch
    dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        true,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        true,                              // runtimeTopicResolution
        "${record:value('/topic')}",                               // topicExpression
        TOPIC8 + ", " + TOPIC9 + ", " + TOPIC10,                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC8, TOPIC9, TOPIC10));
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams8.size() == 1);
    it = kafkaStreams8.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams9.size() == 1);
    it = kafkaStreams9.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams10.size() == 1);
    it = kafkaStreams10.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());
  }

  @Test
  /**
   * Tests runtime topic resolution from record but where white list is "*". All topics should be allowed.
   * Tests for both 'single message per record' and 'single message per batch' options.
   */
  public void testTopicExpression2() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",                               // partition
        kafkaProducerConfig,                               // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        true,                              // runtimeTopicResolution
        "${record:value('/topic')}",                               // topicExpression
        "*",                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC12, TOPIC13, TOPIC14));
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams12.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams12.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams13.size() == 1);
    it = kafkaStreams13.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams14.size() == 1);
    it = kafkaStreams14.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    //single message per batch
    dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",                               // partition
        kafkaProducerConfig,               // kafka producer configs
        true,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        true,                              // runtimeTopicResolution
        "${record:value('/topic')}",       // topicExpression
        "*",
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC12, TOPIC13, TOPIC14));
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams12.size() == 1);
    it = kafkaStreams12.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams13.size() == 1);
    it = kafkaStreams13.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams14.size() == 1);
    it = kafkaStreams14.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());
  }

  @Test
  /**
   * Tests runtime topic resolution from record but where the topic name resolved is not part of the white list
   * All records are sent to error.
   * Tests for both 'single message per record' and 'single message per batch' options.
   */
  public void testTopicExpression3() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",
        kafkaProducerConfig,
        false,
        PartitionStrategy.EXPRESSION,
        true,
        "${record:value('/topic')}",
        TOPIC15,
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC15, "BlackListTopic"));
    targetRunner.runWrite(logRecords);

    Assert.assertEquals(3, targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams15.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams15.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(3, messages.size());

    dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",
        kafkaProducerConfig,
        true,
        PartitionStrategy.EXPRESSION,
        true,
        "${record:value('/topic')}",
        TOPIC15,
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC15, "BlackListTopic"));
    targetRunner.runWrite(logRecords);

    Assert.assertEquals(3, targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams15.size() == 1);
    it = kafkaStreams15.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(1, messages.size());
  }

  @Test
  /**
   * Tests runtime topic resolution from record but where the topic name resolved is allowed but does not exist.
   * All records are sent to error.
   * Tests for both 'single message per record' and 'single message per batch' options.
   */
  public void testTopicExpression4() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",
        kafkaProducerConfig,
        false,
        PartitionStrategy.EXPRESSION,
        true,
        "${record:value('/topic')}",
        "*",
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of("InvalidTopic"));
    targetRunner.runWrite(logRecords);

    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",
        kafkaProducerConfig,
        true,
        PartitionStrategy.EXPRESSION,
        true,
        "${record:value('/topic')}",
        TOPIC15,
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of("InvalidTopic"));
    targetRunner.runWrite(logRecords);

    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();
  }

  @Test
  /**
   * Tests that message with invalid partition is sent to error
   */
  public void testInvalidPartition() throws InterruptedException, StageException, IOException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "${record:value('/partition')}",
        kafkaProducerConfig,
        false,
        PartitionStrategy.EXPRESSION,
        true,
        "${record:value('/topic')}",
        "*",
        new KafkaTargetConfig(),
        DataFormat.SDC_JSON,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicField(ImmutableList.of(TOPIC16));
    targetRunner.runWrite(logRecords);

    Assert.assertEquals(1, targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams16.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams16.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(2, messages.size());
  }

  @Test
  public void testTopicConstant() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    Map<String, Object> constants = new HashMap<>();
    constants.put("TOPIC11", TOPIC11);

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        "${TOPIC11}",
        "0",
        kafkaProducerConfig,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).addConstants(constants).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams11.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams11.get(0).iterator();
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

  @Test(expected = StageException.class)
  /**
   * Tests that KafkaTarget validates the names of the topics present in the white list during init
   */
  public void testInvalidTopicWhiteList() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    //STOP PIPELINE
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",
        kafkaProducerConfig,
        false,
        PartitionStrategy.EXPRESSION,
        true,
        "hello",
        "badTopic",
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE).build();

    targetRunner.runInit();
  }

  @Test
  public void testWriteAvroRecords() throws Exception {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.includeSchema = true;


    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC17,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.AVRO,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();

    List<Record> records = SdcAvroTestUtil.getRecords1();

    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional

    Assert.assertTrue(kafkaStreams17.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams17.get(0).iterator();
    try {
      while (it.hasNext()) {
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
          new SeekableByteArrayInput(it.next().message()), datumReader);
        while(dataFileReader.hasNext()) {
          genericRecords.add(dataFileReader.next());
        }
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteAvroRecordsSingleMessage() throws Exception {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.includeSchema = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC18,
        "0",
        null,
        true,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.AVRO,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();

    List<Record> records = SdcAvroTestUtil.getRecords1();

    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional

    Assert.assertTrue(kafkaStreams18.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams18.get(0).iterator();
    int messageCount = 0;
    try {
      while (it.hasNext()) {
        messageCount++;
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
          new SeekableByteArrayInput(it.next().message()), datumReader);
        while(dataFileReader.hasNext()) {
          genericRecords.add(dataFileReader.next());
        }
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(1, messageCount);
    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteAvroRecordsDropSchema() throws Exception {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.includeSchema = false;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC19,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.AVRO,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();

    List<Record> records = SdcAvroTestUtil.getRecords1();

    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional
    datumReader.setSchema(new Schema.Parser().parse(SdcAvroTestUtil.AVRO_SCHEMA1));
    int messageCounter = 0;

    Assert.assertTrue(kafkaStreams19.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams19.get(0).iterator();
    try {
      while (it.hasNext()) {
        messageCounter++;
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(it.next().message(), null);
        GenericRecord read = datumReader.read(null, decoder);
        genericRecords.add(read);
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(3, messageCounter);
    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteAvroRecordsDropSchemaSingleMessage() throws Exception {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.avroSchema = SdcAvroTestUtil.AVRO_SCHEMA1;
    dataGeneratorFormatConfig.includeSchema = false;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC20,
        "0",
        null,
        true,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.AVRO,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();

    List<Record> records = SdcAvroTestUtil.getRecords1();

    targetRunner.runWrite(records);
    targetRunner.runDestroy();

    List<GenericRecord> genericRecords = new ArrayList<>();
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(); //Reader schema argument is optional
    datumReader.setSchema(new Schema.Parser().parse(SdcAvroTestUtil.AVRO_SCHEMA1));
    int messageCounter = 0;

    Assert.assertTrue(kafkaStreams20.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams20.get(0).iterator();
    try {
      while (it.hasNext()) {
        messageCounter++;
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(it.next().message(), null);
        GenericRecord read = datumReader.read(null, decoder);
        while(read != null) {
          genericRecords.add(read);
          try {
            read = datumReader.read(null, decoder);
          } catch (EOFException e) {
            break;
          }
        }
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }

    Assert.assertEquals(1, messageCounter);
    Assert.assertEquals(3, genericRecords.size());
    SdcAvroTestUtil.compare1(genericRecords);
  }

  @Test
  public void testWriteBinaryRecords() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap<>();
    sdcKafkaTestUtil.setMaxAcks(kafkaProducerConfig);
    kafkaProducerConfig.put("request.timeout.ms", "2000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.binaryFieldPath = "/data";

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC21,
        "0",
        kafkaProducerConfig,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.BINARY,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget).build();

    targetRunner.runInit();
    List<Record> binaryRecords = sdcKafkaTestUtil.createBinaryRecords();
    targetRunner.runWrite(binaryRecords);
    targetRunner.runDestroy();

    List<byte[]> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams21.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams21.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(it.next().message());
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(9, messages.size());
    for(int i = 0; i < binaryRecords.size(); i++) {
      Assert.assertTrue(Arrays.equals(binaryRecords.get(i).get("/data").getValueAsByteArray(), messages.get(i)));
    }
  }
}
