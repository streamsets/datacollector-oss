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
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.KafkaTestUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaServer;
import org.junit.After;
import org.junit.Before;

import java.util.List;

public class TestKafkaSourceUnavailability {

  private static Producer<String, String> producer;

  private static final int PARTITIONS = 1;
  private static final int REPLICATION_FACTOR = 1;
  private static final String CONSUMER_GROUP = "SDC";
  private static KafkaServer kafkaServer;

  @Before
  public void setUp() {
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(1);
    producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), true);
    kafkaServer = KafkaTestUtil.getKafkaServers().get(0);
  }

  @After
  public void tearDown() {
    KafkaTestUtil.shutdown();
  }

  //The test is commented out as they take a long time to complete ~ 30 seconds
  //@Test(expected = StageException.class)
  public void testKafkaServerDown() throws StageException {

    KafkaTestUtil.createTopic("testKafkaServerDown", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testKafkaServerDown",
      String.valueOf(0), 9);
    for (KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testKafkaServerDown")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", KafkaTestUtil.getZkConnect())
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 300000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();
    kafkaServer.shutdown();
    sourceRunner.runProduce(null, 5);
  }

  //The test is commented out as they take a long time to complete ~ 30 seconds
  //@Test(expected = StageException.class)
  public void testZookeeperDown() throws StageException {

    KafkaTestUtil.createTopic("testZookeeperDown", PARTITIONS, REPLICATION_FACTOR);
    List<KeyedMessage<String, String>> data = KafkaTestUtil.produceStringMessages("testZookeeperDown",
      String.valueOf(0), 9);
    for (KeyedMessage<String, String> d : data) {
      producer.send(d);
    }

    SourceRunner sourceRunner = new SourceRunner.Builder(KafkaDSource.class)
      .addOutputLane("lane")
      .addConfiguration("topic", "testZookeeperDown")
      .addConfiguration("consumerGroup", CONSUMER_GROUP)
      .addConfiguration("zookeeperConnect", KafkaTestUtil.getZkConnect())
      .addConfiguration("maxBatchSize", 9)
      .addConfiguration("maxWaitTime", 1000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("kafkaConsumerConfigs", null)
      .build();

    sourceRunner.runInit();
    KafkaTestUtil.getZkServer().shutdown();
    sourceRunner.runProduce(null, 5);
  }
}
