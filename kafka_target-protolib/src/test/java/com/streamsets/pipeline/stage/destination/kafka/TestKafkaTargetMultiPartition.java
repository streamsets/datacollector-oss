/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  private static final String HOST = "localhost";
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

  @BeforeClass
  public static void setUp() {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(3);
    // create topic
    KafkaTestUtil.createTopic(TOPIC1, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC2, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC3, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC4, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC5, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC6, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC7, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC8, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC9, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC10, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC11, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC12, PARTITIONS, REPLICATION_FACTOR);
    KafkaTestUtil.createTopic(TOPIC13, PARTITIONS, REPLICATION_FACTOR);

    kafkaStreams1 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC1, PARTITIONS);
    kafkaStreams2 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC2, PARTITIONS);
    kafkaStreams3 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC3, PARTITIONS);
    kafkaStreams4 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC4, PARTITIONS);
    kafkaStreams5 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC5, PARTITIONS);
    kafkaStreams6 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC6, PARTITIONS);
    kafkaStreams7 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC7, PARTITIONS);
    kafkaStreams8 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC8, PARTITIONS);
    kafkaStreams9 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC9, PARTITIONS);
    kafkaStreams10 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC10, PARTITIONS);
    kafkaStreams11 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC11, PARTITIONS);
    kafkaStreams12 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC12, PARTITIONS);
    kafkaStreams13 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC13, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Test
  public void testWriteStringRecordsRoundRobin() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC1)
      .addConfiguration("partition", "-1")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
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

  @Test
  public void testDefaultPartitioner1() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC12)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/')}")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.DEFAULT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("charset", "UTF-8")
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
    Assert.assertTrue(kafkaStreams12.size() == PARTITIONS);
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams12) {
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
  public void testDefaultPartitioner2() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC13)
        //record has a map which contains an integer field with key "partitionKey",
        //kafka has 3 partitions. Expression distributes the record to partition based on the condition
      .addConfiguration("partition", "${record:value('/')}")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.DEFAULT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createIdenticalStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> records = new ArrayList<>();
    for(Record r : logRecords) {
      records.add(r.get().getValueAsString());
    }
    List<String> messages = new ArrayList<>();
    List<Integer> partitionSize = new ArrayList<>();
    Assert.assertTrue(kafkaStreams13.size() == PARTITIONS);
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
