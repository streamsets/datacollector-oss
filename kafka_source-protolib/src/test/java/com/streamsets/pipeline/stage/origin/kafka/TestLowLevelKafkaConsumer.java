/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.KafkaBroker;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestLowLevelKafkaConsumer {

  private static Producer<String, String> producer;

  private static final String HOST = "localhost";
  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static KafkaServer kafkaServer;
  private static int port;

  @Before
  public void setUp() {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);
    kafkaServer = KafkaTestUtil.getKafkaServers().get(0);
    String[] split = KafkaTestUtil.getMetadataBrokerURI().split(":");
    port = Integer.parseInt(split[1]);
  }

  @After
  public void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Test(expected = StageException.class)
  public void testReadAfterKafkaShutdown() throws Exception {
    KafkaTestUtil.createTopic("testReadAfterZookeeperShutdown", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testReadAfterZookeeperShutdown",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testReadAfterZookeeperShutdown", 0,
      new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //shutdown zookeeper server
    kafkaServer.shutdown();
    //attempt to read
    kafkaConsumer.read(0);
  }

  @Test(expected = StageException.class)
  public void testGetOffsetAfterKafkaShutdown() throws Exception {
    KafkaTestUtil.createTopic("testGetOffsetAfterZookeeperShutdown", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testGetOffsetAfterZookeeperShutdown",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testGetOffsetAfterZookeeperShutdown", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //shutdown zookeeper server
    kafkaServer.shutdown();
    //attempt to read
    kafkaConsumer.getOffsetToRead(true);
  }

  @Test
  public void testReadInvalidOffset() throws Exception {
    KafkaTestUtil.createTopic("testReadInvalidOffset", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testReadInvalidOffset",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testReadInvalidOffset", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //attempt to read invalid offset
    List<MessageAndOffset> read = kafkaConsumer.read(12);
    Assert.assertEquals(0, read.size());
  }

  @Test
  public void testReadValidOffset() throws Exception {
    KafkaTestUtil.createTopic("testReadValidOffset", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testReadValidOffset",
      String.valueOf(0), 9);
    //writes 9 messages to kafka topic
    for(KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    LowLevelKafkaConsumer kafkaConsumer = new LowLevelKafkaConsumer("testReadValidOffset", 0, new KafkaBroker(HOST, port), 0, 8000,
      2000, "testKafkaConsumer" + "_client");
    kafkaConsumer.init();
    //attempt to read invalid offset
    List<MessageAndOffset> read = kafkaConsumer.read(6);
    Assert.assertEquals(3, read.size());
  }
}
