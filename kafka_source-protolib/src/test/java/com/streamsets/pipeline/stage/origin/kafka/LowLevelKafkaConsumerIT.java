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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.LowLevelConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumerFactory;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtil;
import com.streamsets.pipeline.kafka.common.SdcKafkaTestUtilFactory;
import com.streamsets.testing.SingleForkNoReuseTest;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

@Category(SingleForkNoReuseTest.class)
public class LowLevelKafkaConsumerIT {

  private static Producer<String, String> producer;

  private static final String HOST = "localhost";
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static KafkaServer kafkaServer;
  private static int port;
  private static final SdcKafkaTestUtil sdcKafkaTestUtil = SdcKafkaTestUtilFactory.getInstance().create();

  @BeforeClass
  public static void setUp() throws IOException {
    sdcKafkaTestUtil.startZookeeper();
    sdcKafkaTestUtil.startKafkaBrokers(1);
    producer = sdcKafkaTestUtil.createProducer(sdcKafkaTestUtil.getMetadataBrokerURI(), true);
    kafkaServer = sdcKafkaTestUtil.getKafkaServers().get(0);
    String[] split = sdcKafkaTestUtil.getMetadataBrokerURI().split(":");
    port = Integer.parseInt(split[1]);
  }

  @AfterClass
  public static void tearDown() {
    sdcKafkaTestUtil.shutdown();
  }

  @Test(expected = StageException.class)
  public void testReadAfterKafkaShutdown() throws Exception {
    sdcKafkaTestUtil.createTopic("testReadAfterZookeeperShutdown", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = sdcKafkaTestUtil.produceStringMessages("testReadAfterZookeeperShutdown",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SdcKafkaLowLevelConsumer kafkaConsumer = createLowLevelConsumer("testReadAfterZookeeperShutdown");

    kafkaConsumer.init();
    //shutdown zookeeper server
    kafkaServer.shutdown();
    try {
      //attempt to read
      kafkaConsumer.read(0);
    } finally {
      kafkaServer.startup();
    }
  }

  @Test(expected = StageException.class)
  public void testGetOffsetAfterKafkaShutdown() throws Exception {
    sdcKafkaTestUtil.createTopic("testGetOffsetAfterZookeeperShutdown", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = sdcKafkaTestUtil.produceStringMessages("testGetOffsetAfterZookeeperShutdown",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SdcKafkaLowLevelConsumer kafkaConsumer = createLowLevelConsumer("testGetOffsetAfterZookeeperShutdown");
    kafkaConsumer.init();
    //shutdown zookeeper server
    kafkaServer.shutdown();
    //attempt to read
    try {
      //attempt to read
      kafkaConsumer.read(0);
    } finally {
      kafkaServer.startup();
    }
  }

  @Test
  public void testReadInvalidOffset() throws Exception {
    sdcKafkaTestUtil.createTopic("testReadInvalidOffset", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = sdcKafkaTestUtil.produceStringMessages("testReadInvalidOffset",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SdcKafkaLowLevelConsumer kafkaConsumer = createLowLevelConsumer("testReadInvalidOffset");
    kafkaConsumer.init();
    //attempt to read invalid offset
    List<MessageAndOffset> read = kafkaConsumer.read(12);
    Assert.assertEquals(0, read.size());
  }

  @Test
  public void testReadValidOffset() throws Exception {
    sdcKafkaTestUtil.createTopic("testReadValidOffset", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = sdcKafkaTestUtil.produceStringMessages("testReadValidOffset",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SdcKafkaLowLevelConsumer kafkaConsumer = createLowLevelConsumer("testReadValidOffset");
    kafkaConsumer.init();
    //attempt to read invalid offset
    List<MessageAndOffset> read = kafkaConsumer.read(6);
    Assert.assertEquals(3, read.size());
  }

  private SdcKafkaLowLevelConsumer createLowLevelConsumer(String topic) {
    LowLevelConsumerFactorySettings lowLevelConsumerFactorySettings = new LowLevelConsumerFactorySettings(
      topic,
      0,
      HOST,
      port,
      "testKafkaLowLevelConsumer",
      0,
      8000,
      2000
    );
    SdcKafkaLowLevelConsumerFactory sdcKafkaLowLevelConsumerFactory =
      SdcKafkaLowLevelConsumerFactory.create(lowLevelConsumerFactorySettings);
    return sdcKafkaLowLevelConsumerFactory.create();
  }

}
