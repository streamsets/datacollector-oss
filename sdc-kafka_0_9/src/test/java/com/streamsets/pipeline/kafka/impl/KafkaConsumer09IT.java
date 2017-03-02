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
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaConsumer09IT {

  private static final int NUM_MESSAGES = 10;

  @Test
  public void testKafkaConsumer09Version() throws IOException {
    Source.Context sourceContext = ContextInfoCreator.createSourceContext(
        "s",
        false,
        OnRecordError.TO_ERROR,
        ImmutableList.of("a")
    );

    SdcKafkaConsumer sdcKafkaConsumer = createSdcKafkaConsumer("", "", 0, sourceContext, Collections.emptyMap(), "");
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

    SdcKafkaConsumer sdcKafkaConsumer = createKafkaConsumer(port, topic, sourceContext);

    // produce some messages to topic
    produce(topic, "localhost:" + port, message);

    // read
    List<MessageAndOffset> read = new ArrayList<>();
    while(read.size() < NUM_MESSAGES) {
      MessageAndOffset messageAndOffset = sdcKafkaConsumer.read();
      if(messageAndOffset != null) {
        read.add(messageAndOffset);
      }
    }
    // verify
    Assert.assertNotNull(read);
    Assert.assertEquals(NUM_MESSAGES, read.size());
    verify(read, message);

    // delete topic and shutdown
    AdminUtils.deleteTopic(
      zkUtils,
      topic
    );
    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  @Test
  public void testAssignedPartitionsOnRebalance() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    final int numPartitions = 2;
    final int numConsumers = 2;

    final String topic = "TestKafkaConsumerRebalance";
    final String message = "Hello StreamSets";

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
        zkConnect, zkSessionTimeout, zkConnectionTimeout,
        JaasUtils.isZkSecurityEnabled());

    int port = TestUtil.getFreePort();

    KafkaServer kafkaServer = TestUtil.createKafkaServer(port, zkConnect, true, numPartitions);

    List<SdcKafkaConsumer> consumers = new ArrayList<>();

    for (int i=0; i<numConsumers; i++) {
      Source.Context sourceContext = ContextInfoCreator.createSourceContext(
          "s",
          false,
          OnRecordError.TO_ERROR,
          ImmutableList.of("a")
      );

      consumers.add(createKafkaConsumer(port, topic, sourceContext));

      // allow rebalance to happen
      ThreadUtil.sleep(10000);

      for (int c=0; c<consumers.size(); c++) {
        SdcKafkaConsumer consumer = consumers.get(c);

        for (int x=0; x<=consumers.size(); x++) {
          for (int j = 0; j < numPartitions; j++) {
            produce(topic, "localhost:" + port, message, j);
          }
        }
        List<MessageAndOffset> read = new ArrayList<>();
        while(read.size() < NUM_MESSAGES * numPartitions) {
          MessageAndOffset messageAndOffset = consumer.read();
          if(messageAndOffset != null) {
            read.add(messageAndOffset);
          } else {
            Assert.fail("Consumer failed to read record");
          }
        }

        Assert.assertNotNull(read);
        Assert.assertEquals(NUM_MESSAGES * numPartitions, read.size());
      }

      Set<Integer> partitions = new HashSet<>();
      for (SdcKafkaConsumer consumer : consumers) {
        consumer.commit();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry :
            ((BaseKafkaConsumer09)consumer).getTopicPartitionToOffsetMetadataMap().entrySet()) {
          final int partition = entry.getKey().partition();
          if (!partitions.add(partition)) {
            Assert.fail(String.format("Partition %d was committed by more than one consumer", partition));
          }
        }
      }
    }

    // delete topic and shutdown
    AdminUtils.deleteTopic(
        zkUtils,
        topic
    );
    kafkaServer.shutdown();
    zookeeper.shutdown();
  }

  @NotNull
  private SdcKafkaConsumer createKafkaConsumer(int port, String topic, Source.Context sourceContext) throws
      StageException {
    Map<String, Object> props = new HashMap<>();
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    props.put(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
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
    return sdcKafkaConsumer;
  }

  private void verify(List<MessageAndOffset> read, String message) {
    for(int i = 0; i < read.size(); i++) {
      Assert.assertEquals(message+i, new String((byte[])read.get(i).getPayload()));
    }
  }

  private void produce(String topic, String bootstrapServers, String message) {
    produce(topic, bootstrapServers, message, null);
  }

  private void produce(String topic, String bootstrapServers, String message, Integer partition) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put(KafkaConstants.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(KafkaConstants.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    for(int i = 0; i < NUM_MESSAGES; i++) {
      ProducerRecord<String, String> record;
      if (partition != null) {
        record = new ProducerRecord<>(topic, partition, Integer.toString(0), message + i);
      } else {
        record = new ProducerRecord<>(topic, Integer.toString(0), message + i);
      }
      producer.send(record);
    }
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
