/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.kafka.cluster;

import com.google.common.io.Resources;
import com.streamsets.dc.base.TestPipelineOperationsCluster;
import com.streamsets.pipeline.lib.DataType;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.lib.ProducerRunnable;
import kafka.javaapi.producer.Producer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class TestKafkaOriginSinglePartition extends TestPipelineOperationsCluster {

  private static final String TOPIC = "TestKafkaOriginSinglePartitionCluster";
  private static CountDownLatch startLatch;
  private static ExecutorService executorService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    KafkaTestUtil.createTopic(TOPIC, 1, 1);
    startLatch = new CountDownLatch(1);
    Producer<String, String> producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);
    executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC, 1, producer, startLatch, DataType.TEXT, null, -1,
      null));

    TestPipelineOperationsCluster.beforeClass(getPipelineJson(), "TestKafkaOriginSinglePartitionCluster");
    startLatch.countDown();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    executorService.shutdownNow();
    KafkaTestUtil.shutdown();
    TestPipelineOperationsCluster.afterClass("TestKafkaOriginSinglePartitionCluster");
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_origin_pipeline.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
    pipelineJson = pipelineJson.replaceAll("STANDALONE", "CLUSTER");
    return pipelineJson;
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
