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

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.kafka.exception.KafkaConnectionException;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.kafka.util.KafkaTargetUtil;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.consumer.KafkaStream;
import kafka.server.KafkaServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category(SingleForkNoReuseTest.class)
public class KafkaTargetUnavailabilityIT {

  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;

  private static final String HOST = "localhost";
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "KafkaTargetUnavailabilityIT";
  private static KafkaServer kafkaServer;
  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  @Before
  public void setUp() throws IOException {
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(1);
    kafkaServer = sdcKafkaTestUtil.getKafkaServers().get(0);
    // create topic
    sdcKafkaTestUtil.createTopic(TOPIC, PARTITIONS, REPLICATION_FACTOR);

    kafkaStreams = sdcKafkaTestUtil.createKafkaStream(sdcKafkaTestUtil.getZkConnect(), TOPIC, PARTITIONS);
  }

  @After
  public void tearDown() {
    sdcKafkaTestUtil.shutdown();
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
    kafkaProducerConfig.put("request.required.acks", "-1");
    kafkaProducerConfig.put("acks", "all");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/text";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    //STOP PIPELINE
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();

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
    kafkaProducerConfig.put("request.required.acks", "-1");
    kafkaProducerConfig.put("acks", "all");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/text";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    //STOP PIPELINE
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();

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
    kafkaProducerConfig.put("request.required.acks", "-1");
    kafkaProducerConfig.put("acks", "all");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/text";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        false,
        null,
        null,
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    //STOP PIPELINE
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.DISCARD)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();

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
    kafkaProducerConfig.put("request.required.acks", "-1");
    kafkaProducerConfig.put("acks", "all");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/text";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        true,
        "test",
        "*",
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    //TO ERROR
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();

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
    kafkaProducerConfig.put("request.required.acks", "-1");
    kafkaProducerConfig.put("acks", "all");
    kafkaProducerConfig.put("request.timeout.ms", "2000");
    kafkaProducerConfig.put("message.send.max.retries", "10");
    kafkaProducerConfig.put("retry.backoff.ms", "1000");

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        null,
        "0",
        null,
        false,
        PartitionStrategy.EXPRESSION,
        true,
        "test",
        "*",
        new KafkaTargetConfig(),
        DataFormat.TEXT,
        dataGeneratorFormatConfig
    );

    //TO ERROR
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class, kafkaTarget)
      .setOnRecordError(OnRecordError.DISCARD)
      .build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();

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

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/text";
    dataGeneratorFormatConfig.textEmptyLineIfNull = true;

    KafkaTarget kafkaTarget = KafkaTargetUtil.createKafkaTarget(
        sdcKafkaTestUtil.getMetadataBrokerURI(),
        TOPIC,
        "0",
        null,
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
      .build();

    targetRunner.runInit();
    List<Record> logRecords = sdcKafkaTestUtil.createStringRecords();

    sdcKafkaTestUtil.shutdown();
    Thread.sleep(500);
    try {
      targetRunner.runWrite(logRecords);
    } catch (StageException e) {
      Assert.assertEquals(KafkaErrors.KAFKA_50, e.getErrorCode());
    }
    targetRunner.runDestroy();
  }
}
