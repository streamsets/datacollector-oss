/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.kafka.standalone;

import com.google.common.io.Resources;
import com.streamsets.pipeline.base.TestPipelineOperationsStandalone;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestKafkaDestinationMultiPartitionPipelineOperations extends TestPipelineOperationsStandalone {

  private static final String TOPIC = "TestKafkaDestinationMultiPartitionPipelineOperations";

  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(3);
    KafkaTestUtil.createTopic(TOPIC, 3, 2);
    TestPipelineOperationsStandalone.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    KafkaTestUtil.shutdown();
    TestPipelineOperationsStandalone.afterClass();
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_destination_pipeline_operations.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkServer().connectString());
    return pipelineJson;
  }

  @Override
  protected String getPipelineName() {
    return "kafka_destination_pipeline";
  }

  @Override
  protected String getPipelineRev() {
    return "1";
  }

  @Override
  protected void postPipelineStart() {

  }

}
