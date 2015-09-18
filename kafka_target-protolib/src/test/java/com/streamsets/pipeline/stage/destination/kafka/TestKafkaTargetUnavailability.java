/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.KafkaConnectionException;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import kafka.consumer.KafkaStream;
import kafka.server.KafkaServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestKafkaTargetUnavailability {

  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;

  private static final String HOST = "localhost";
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "TestKafkaTargetUnavailability";
  private static KafkaServer kafkaServer;

  @Before
  public void setUp() {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    kafkaServer = KafkaTestUtil.getKafkaServers().get(0);
    // create topic
    KafkaTestUtil.createTopic(TOPIC, PARTITIONS, REPLICATION_FACTOR);

    kafkaStreams = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC, PARTITIONS);
  }

  @After
  public void tearDown() {
    KafkaTestUtil.shutdown();
  }

  //The following tests are commented out as they take a long time to complete ~ 10 seconds

  //@Test
  /**
   * Simulate scenario where kafka server shuts down after successful initialization but before producing any records
   * and the OnRecordError config is set to STOP_PIPELINE.
   *
   * A KafkaConnectionException is expected with error code KAFKA_50
   */
  public void testKafkaServerDownStopPipeline() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    //STOP PIPELINE
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
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

    kafkaServer.shutdown();

    try {
      targetRunner.runWrite(logRecords);
      Assert.fail("Expected StageException, got none.");
    } catch (KafkaConnectionException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_50, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //@Test
  /**
   * Simulate scenario where kafka server shuts down after successful initialization but before producing any records
   * and the OnRecordError config is set to TO_ERROR.
   *
   * A KafkaConnectionException is expected with error code KAFKA_50
   */
  public void testKafkaServerDownToError() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    //STOP PIPELINE
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
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

    kafkaServer.shutdown();

    try {
      targetRunner.runWrite(logRecords);
      Assert.fail("Expected StageException, got none.");
    } catch (KafkaConnectionException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_50, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //@Test
  /**
   * Simulate scenario where kafka server shuts down after successful initialization but before producing any records
   * and the OnRecordError config is set to DISCARD.
   *
   * A KafkaConnectionException is expected with error code KAFKA_50
   */
  public void testKafkaServerDownDiscard() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    //STOP PIPELINE
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.DISCARD)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
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

    kafkaServer.shutdown();

    try {
      targetRunner.runWrite(logRecords);
      Assert.fail("Expected StageException, got none.");
    } catch (KafkaConnectionException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_50, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //@Test
  /**
   * Simulate scenario where kafka server shuts down after successful initialization but before producing any records
   * and the OnRecordError config is set to TO_ERROR.
   *
   * A KafkaConnectionException is expected with error code KAFKA_67 as the failure occurs when sdc kafka producer
   * tries to validate the topic name at runtime.
   */
  public void testKafkaServerDownToErrorDynamicTopicResolution() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    //TO ERROR
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("topic", null)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", true)
      .addConfiguration("topicExpression", "test")
      .addConfiguration("topicWhiteList", "*")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();

    kafkaServer.shutdown();

    try {
      targetRunner.runWrite(logRecords);
      Assert.fail("Expected StageException, got none.");
    } catch (KafkaConnectionException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_67, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //@Test
  /**
   * Simulate scenario where kafka server shuts down after successful initialization but before producing any records
   * and the OnRecordError config is set to DISCARD.
   *
   * A KafkaConnectionException is expected with error code KAFKA_67 as the failure occurs when sdc kafka producer
   * tries to validate the topic name at runtime.
   */
  public void testKafkaServerDownDiscardDynamicTopicResolution() throws InterruptedException, StageException {

    Map<String, String> kafkaProducerConfig = new HashMap();
    kafkaProducerConfig.put("request.required.acks", "2");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    //TO ERROR
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .setOnRecordError(OnRecordError.DISCARD)
      .addConfiguration("topic", null)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", kafkaProducerConfig)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("singleMessagePerBatch", false)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("runtimeTopicResolution", true)
      .addConfiguration("topicExpression", "test")
      .addConfiguration("topicWhiteList", "*")
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();

    kafkaServer.shutdown();

    try {
      targetRunner.runWrite(logRecords);
      Assert.fail("Expected StageException, got none.");
    } catch (KafkaConnectionException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_67, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //The test is commented out as they take a long time to complete ~ 5 seconds
  //@Test
  public void testZookeeperDown() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("payloadType", DataFormat.TEXT)
      .addConfiguration("partitionStrategy", PartitionStrategy.EXPRESSION)
      .addConfiguration("constants", null)
      .addConfiguration("csvFileFormat", "DEFAULT")
      .addConfiguration("runtimeTopicResolution", false)
      .addConfiguration("topicExpression", null)
      .addConfiguration("topicWhiteList", null)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = KafkaTestUtil.createStringRecords();

    KafkaTestUtil.getZkServer().shutdown();
    Thread.sleep(500);
    try {
      targetRunner.runWrite(logRecords);
    } catch (StageException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_50, e.getErrorCode());
    }
    targetRunner.runDestroy();
  }
}
