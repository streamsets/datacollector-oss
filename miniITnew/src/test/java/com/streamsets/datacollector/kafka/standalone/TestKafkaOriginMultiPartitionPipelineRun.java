/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.kafka.standalone;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.TestPipelineRunStandalone;
import com.streamsets.pipeline.lib.KafkaTestUtil;

import kafka.javaapi.producer.Producer;

import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestKafkaOriginMultiPartitionPipelineRun extends TestPipelineRunStandalone {

  private static final String TOPIC = "TestKafkaOriginMultiPartition";

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(5);
    KafkaTestUtil.createTopic(TOPIC, 3, 2);
    Producer<String, String> producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), false);
    producer.send(KafkaTestUtil.produceStringMessages(TOPIC, 3, 1000));
  }

  @After
  @Override
  public void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Override
  protected String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_origin_pipeline_standalone.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
    return pipelineJson;
  }

  @Override
  protected int getRecordsInOrigin() {
    return 1000;
  }

  @Override
  protected int getRecordsInTarget() {
    return 1000;
  }

  @Override
  protected String getPipelineName() {
    return "kafka_origin_pipeline";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }
}
