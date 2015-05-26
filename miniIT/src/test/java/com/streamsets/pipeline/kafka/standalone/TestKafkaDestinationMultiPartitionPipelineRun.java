/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.kafka.standalone;

import com.google.common.io.Resources;
import com.streamsets.pipeline.base.TestPipelineRunStandalone;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class TestKafkaDestinationMultiPartitionPipelineRun extends TestPipelineRunStandalone {

  private static final String TOPIC = "TestKafkaDestinationMultiPartition";
  private static List<KafkaStream<byte[], byte[]>> kafkaStreams;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(3);
    KafkaTestUtil.createTopic(TOPIC, 3, 2);
    kafkaStreams = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC, 3);
  }

  @After
  @Override
  public void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Override
  protected String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_destination_pipeline_run.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkServer().connectString());
    return pipelineJson;
  }

  @Override
  protected int getRecordsInOrigin() {
    return 500;
  }

  @Override
  protected int getRecordsInTarget() {
    int expectedRecordsInTarget = 0;
    for(KafkaStream<byte[], byte[]> kafkaStream : kafkaStreams) {
      ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
      try {
        while (it.hasNext()) {
          expectedRecordsInTarget++;
          it.next();
        }
      } catch (kafka.consumer.ConsumerTimeoutException e) {
        //no-op
      }
    }
    return expectedRecordsInTarget;
  }
}
