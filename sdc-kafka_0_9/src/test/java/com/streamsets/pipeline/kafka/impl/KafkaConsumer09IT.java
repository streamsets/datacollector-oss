/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.kafka.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.ConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.MessageAndOffset;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumerFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer09IT {

  @Test
  public void testKafkaConsumer09Version() throws IOException {
    SdcKafkaConsumer sdcKafkaConsumer = createSdcKafkaConsumer("", "", 0, null, null, "");
    Assert.assertEquals(Kafka09Constants.KAFKA_VERSION, sdcKafkaConsumer.getVersion());
  }

  @Test
  public void testKafkaConsumer09Read() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
      zkConnect, zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled());

    int port = TestUtil.getFreePort();
    KafkaServer kafkaServer = TestUtil.createKafkaServer(port, zkConnect);

    final String topic = "TestKafkaConsumer09_1";
    final String message = "Hello StreamSets";

    Source.Context sourceContext = ContextInfoCreator.createSourceContext(
      "s",
      false,
      OnRecordError.TO_ERROR,
      ImmutableList.of("a")
    );

    Map<String, Object> props = new HashMap<>();
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    SdcKafkaConsumer sdcKafkaConsumer = createSdcKafkaConsumer(
        "localhost:" + port,
        topic,
        1000,
        sourceContext,
        props,
        "test"
    );
    sdcKafkaConsumer.validate(new ArrayList<Stage.ConfigIssue>(), sourceContext);
    sdcKafkaConsumer.init();

    // produce some messages to topic
    produce(topic, "localhost:" + port, message);

    // read
    List<MessageAndOffset> read = new ArrayList<>();
    while(read.size() < 10) {
      MessageAndOffset messageAndOffset = sdcKafkaConsumer.read();
      if(messageAndOffset != null) {
        read.add(messageAndOffset);
      }
    }
    // verify
    Assert.assertNotNull(read);
    Assert.assertEquals(10, read.size());
    verify(read, message);

    // delete topic and shutdown
    AdminUtils.deleteTopic(
      zkUtils,
      topic
    );
    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  private void verify(List<MessageAndOffset> read, String message) {
    for(int i = 0; i < read.size(); i++) {
      Assert.assertEquals(message+i, new String(read.get(i).getPayload()));
    }
  }

  private void produce(String topic, String bootstrapServers, String message) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    for(int i = 0; i < 10; i++)
      producer.send(new ProducerRecord<>(topic, Integer.toString(0), message+i));
    producer.close();

  }

  private SdcKafkaConsumer createSdcKafkaConsumer(
      String bootstrapServers,
      String topic,
      int maxWaitTime,
      Source.Context context,
      Map<String, Object> kafkaConfigs,
      String group
  ) {
    ConsumerFactorySettings settings = new ConsumerFactorySettings(
      "", //zkconnect
      bootstrapServers,
      topic,
      maxWaitTime,
      context,
      kafkaConfigs,
      group
    );
    SdcKafkaConsumerFactory sdcKafkaConsumerFactory = SdcKafkaConsumerFactory.create(settings);
    return sdcKafkaConsumerFactory.create();
  }
}
