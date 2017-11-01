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
package com.streamsets.datacollector.kafka.standalone;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.PipelineOperationsStandaloneIT;
import com.streamsets.pipeline.kafka.common.DataType;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;
import com.streamsets.pipeline.kafka.common.ProducerRunnable;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import kafka.javaapi.producer.Producer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Ignore
public class KafkaOriginMultiPartitionPipelineOperationsIT extends PipelineOperationsStandaloneIT {

  private static final String TOPIC = "TestKafkaOriginMultiPartitionPipelineOperations";
  private static CountDownLatch startLatch;
  private static ExecutorService executorService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(5);
    KafkaTestUtil.createTopic(TOPIC, 3, 2);
    startLatch = new CountDownLatch(1);
    Producer<String, String> producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);
    executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new ProducerRunnable(TOPIC, 3, producer, startLatch, DataType.TEXT, null, -1,
      null, SdcKafkaTestUtilFactory.getInstance().create()));
    PipelineOperationsStandaloneIT.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (executorService != null) {
      executorService.shutdownNow();
    }
    KafkaTestUtil.shutdown();
    PipelineOperationsStandaloneIT.afterClass();
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_origin_pipeline_standalone.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
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

  @Override
  protected void postPipelineStart() {
    startLatch.countDown();
  }

}
