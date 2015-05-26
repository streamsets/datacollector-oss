/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.Record;
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

import java.util.ArrayList;
import java.util.List;

public class TestToErrorKafkaDTarget {

  private static List<KafkaStream<byte[], byte[]>> kafkaStreams1;

  private static final String HOST = "localhost";
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String TOPIC1 = "TestToErrorKafkaDTarget1";

  @BeforeClass
  public static void setUp() {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    // create topic
    KafkaTestUtil.createTopic(TOPIC1, PARTITIONS, REPLICATION_FACTOR);

    kafkaStreams1 = KafkaTestUtil.createKafkaStream(KafkaTestUtil.getZkServer().connectString(), TOPIC1, PARTITIONS);
  }

  @AfterClass
  public static void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Test
  public void testWriteNoRecords() throws InterruptedException, StageException {
    TargetRunner targetRunner = new TargetRunner.Builder(KafkaDTarget.class)
      .addConfiguration("topic", TOPIC1)
      .addConfiguration("partition", "0")
      .addConfiguration("metadataBrokerList", KafkaTestUtil.getMetadataBrokerURI())
      .addConfiguration("kafkaProducerConfigs", null)
      .addConfiguration("dataFormat", DataFormat.SDC_JSON)
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
    List<Record> logRecords = KafkaTestUtil.createEmptyLogRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    List<String> messages = new ArrayList<>();
    Assert.assertTrue(kafkaStreams1.size() == 1);
    ConsumerIterator<byte[], byte[]> it = kafkaStreams1.get(0).iterator();
    try {
      while (it.hasNext()) {
        messages.add(new String(it.next().message()));
      }
    } catch (kafka.consumer.ConsumerTimeoutException e) {
      //no-op
    }
    Assert.assertEquals(0, messages.size());
  }

}
