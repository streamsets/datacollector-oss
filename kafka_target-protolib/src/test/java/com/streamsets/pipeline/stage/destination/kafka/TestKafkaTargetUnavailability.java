/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.Errors;
import com.streamsets.pipeline.lib.KafkaConnectionException;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
import kafka.admin.AdminUtils;
import kafka.consumer.KafkaStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestKafkaTargetUnavailability {

  private static KafkaServer kafkaServer;
  private static ZkClient zkClient;
  private static EmbeddedZookeeper zkServer;
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;
  private static int port;

  private static final String HOST = "localhost";
  private static final int BROKER_ID = 0;
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC = "TestKafkaTargetUnavailability";
  private static final int TIME_OUT = 5000;

  @Before
  public void setUp() {
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

  @After
  public void tearDown() {
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
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
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
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
      Assert.assertEquals(Errors.KAFKA_50, e.getErrorCode());
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
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
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
      Assert.assertEquals(Errors.KAFKA_50, e.getErrorCode());
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
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
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
      Assert.assertEquals(Errors.KAFKA_50, e.getErrorCode());
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
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
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
      Assert.assertEquals(Errors.KAFKA_67, e.getErrorCode());
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
      .addConfiguration("metadataBrokerList", HOST + ":" + port)
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
      Assert.assertEquals(Errors.KAFKA_67, e.getErrorCode());
    }

    targetRunner.runDestroy();
  }

  //The test is commented out as they take a long time to complete ~ 5 seconds
  //@Test
  public void testZookeeperDown() throws InterruptedException, StageException {

    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC)
      .addConfiguration("partition", "0")
      .addConfiguration("brokerHost", HOST)
      .addConfiguration("brokerPort", port)
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

    zkServer.shutdown();
    Thread.sleep(500);
    try {
      targetRunner.runWrite(logRecords);
    } catch (StageException e) {
      Assert.assertEquals(Errors.KAFKA_50, e.getErrorCode());
    }
    targetRunner.runDestroy();
  }
}
