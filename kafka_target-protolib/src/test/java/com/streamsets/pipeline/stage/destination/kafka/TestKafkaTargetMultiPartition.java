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
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.kafka.util.KafkaTargetUtil;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.TestUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Category(SingleForkNoReuseTest.class)
public class TestKafkaTargetMultiPartition {

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

  private static final int PARTITIONS = 3;
  private static final int REPLICATION_FACTOR = 2;
  private static final String TOPIC1 = "TestKafkaTargetMultiPartition1";
  private static final String TOPIC2 = "TestKafkaTargetMultiPartition2";
  private static final String TOPIC3 = "TestKafkaTargetMultiPartition3";
  private static final String TOPIC4 = "TestKafkaTargetMultiPartition4";
  private static final String TOPIC5 = "TestKafkaTargetMultiPartition5";
  private static final String TOPIC6 = "TestKafkaTargetMultiPartition6";
  private static final String TOPIC7 = "TestKafkaTargetMultiPartition7";
  private static final String TOPIC8 = "TestKafkaTargetMultiPartition8";
  private static final String TOPIC9 = "TestKafkaTargetMultiPartition9";
  private static final String TOPIC10 = "TestKafkaTargetMultiPartition10";
  private static final String TOPIC11 = "TestKafkaTargetMultiPartition11";
  private static final String TOPIC12 = "TestKafkaTargetMultiPartition12";
  private static final String TOPIC13 = "TestKafkaTargetMultiPartition13";

  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(3);
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

    for (int i = 1; i <= 13 ; i++) {
      for (int j = 0; j < PARTITIONS; j++) {
        TestUtils.waitUntilMetadataIsPropagated(
            scala.collection.JavaConversions.asScalaBuffer(sdcKafkaTestUtil.getKafkaServers()),
            "TestKafkaTargetMultiPartition" + String.valueOf(i), j, 5000);
      }
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
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
  }

  //@Test
  public void testWriteStringRecordsRoundRobin() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC1,
        "-1",                               // partition
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
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams1.size());
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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC2,
        "-1",                               // partition
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.RANDOM,
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

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams2.size());
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
        Assert.assertTrue(Utils.format("{} not in {}", message, StringUtils.join(records, ",")), records.contains(message));
        records.remove(message);
      }
      messages.clear();
    }

    //make sure we have seen all the records.
    Assert.assertEquals(0, records.size());
  }

  @Test
  public void testExpressionPartitioner() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC3,
        "${record:value('/') % 3}",
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
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
    List<Record> logRecords = sdcKafkaTestUtil.createIntegerRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams3.size());
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams3) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()).trim());
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      Assert.assertEquals(3, messages.size());
      for(String message : messages) {
        Assert.assertTrue(Utils.format("{} not in {}", message, StringUtils.join(records, ",")), records.contains(message));
      }
      messages.clear();
    }
  }

  @Test
  public void testInvalidPartitionExpression() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC4,
        "${value('/') % 3}",                               // invalid partition expression
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
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

    List<Stage.ConfigIssue> configIssues = targetRunner.runValidateConfigs();
    Assert.assertEquals(1, configIssues.size());

  }

  @Test
  public void testPartitionExpressionEvaluationError() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC5,
        "${record:value('/') % 3}",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
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
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();

  }

  @Test
  public void testPartitionNumberOutOfRange() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC6,
        "13",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
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
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();
  }

  @Test
  public void testInvalidPartition() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC7,
        "${record:value('/')}",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
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
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    Assert.assertNotNull(targetRunner.getErrorRecords());
    Assert.assertTrue(!targetRunner.getErrorRecords().isEmpty());
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());
    targetRunner.runDestroy();

  }

  @Test
  public void testExpressionPartitionerSingleMessage() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC8,
        "${record:value('/') % 3}",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        true,                              // singleMessagePerBatch
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
    List<Record> logRecords = sdcKafkaTestUtil.createIntegerRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams8.size());
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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/topic";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "${record:value('/partition') % 3}",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        true,                              // singleMessagePerBatch
        PartitionStrategy.EXPRESSION,
        true,                              // runtimeTopicResolution
        "${record:value('/topic')}",                               // topicExpression
        "*",                               // topic white list
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createJsonRecordsWithTopicPartitionField(ImmutableList.of(TOPIC9, TOPIC10, TOPIC11),
      PARTITIONS);
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get("/topic").getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams9.size());
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
    Assert.assertEquals(PARTITIONS, kafkaStreams10.size());
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
    Assert.assertEquals(PARTITIONS, kafkaStreams11.size());
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

  @Test
  public void testDefaultPartitioner1() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC12,
        "${record:value('/')}",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.DEFAULT,
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
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams12.size());
    int totalCount = 0;
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams12) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      totalCount += messages.size();
      for(String message : messages) {
        Assert.assertTrue(records.contains(message.trim()));
      }
      messages.clear();
    }
    Assert.assertEquals(9, totalCount);
  }

  @Test
  public void testDefaultPartitioner2() throws InterruptedException, StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.charset = "UTF-8";
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC13,
        "${record:value('/')}",
        sdcKafkaTestUtil.setMaxAcks(new HashMap<String, String>()), // kafka producer configs
        false,                              // singleMessagePerBatch
        PartitionStrategy.DEFAULT,
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
    List<Record> logRecords = sdcKafkaTestUtil.createIdenticalStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    List<Integer> partitionSize = new ArrayList<>();
    Assert.assertEquals(PARTITIONS, kafkaStreams13.size());
    //All messages should be routed to same partition as the partition key is the same
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams13) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          messages.add(new String(it.next().message()));
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
      partitionSize.add(messages.size());
      messages.clear();
    }

    //make sure that partitionSize list has 3 entries - 0, 0 and 9 in no particular order
    Assert.assertEquals(3, partitionSize.size());
    int numberOfMessages =  partitionSize.get(0) + partitionSize.get(1) + partitionSize.get(2);
    Assert.assertEquals(9, numberOfMessages);
    if(partitionSize.get(0) == 9 || partitionSize.get(1) == 9 || partitionSize.get(2) == 9) {

    } else {
      Assert.fail("Only one of the 3 partitions should get all the messages");
    }

  }
}
