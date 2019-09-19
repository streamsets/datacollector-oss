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
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.lib.kafka.KafkaConstants;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.testing.NetworkUtils;
import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class KafkaNewConsumerITBase {

  protected static final int MAX_NUM_SLEEPS = 5;
  protected static final int NUM_MESSAGES = 10;

  protected static final String REBALANCE_TOPIC = "TestKafkaConsumerRebalance";
  protected static final String CONSUMER_GROUP_NAME = "testConsumerRebalance";

  protected static final Logger LOG = LoggerFactory.getLogger(KafkaNewConsumerITBase.class);

  protected abstract KafkaServer buildKafkaServer(int port, String zkConnect, int numPartitions);

  @Test
  public void testAssignedPartitionsOnRebalance() throws IOException, StageException {
    int zkConnectionTimeout = 6000;
    int zkSessionTimeout = 6000;

    final int numPartitions = 2;
    final int numPhases = 2;

    final String topic = REBALANCE_TOPIC;
    final String message = "Hello StreamSets";

    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    String zkConnect = String.format("127.0.0.1:%d", zookeeper.port());
    ZkUtils zkUtils = ZkUtils.apply(
        zkConnect, zkSessionTimeout, zkConnectionTimeout,
        JaasUtils.isZkSecurityEnabled());

    int port = NetworkUtils.getRandomPort();

    KafkaServer kafkaServer = buildKafkaServer(port, zkConnect, numPartitions);

    List<SdcKafkaConsumer> consumers = new ArrayList<>();

    for (int phase=0; phase < numPhases; phase++) {
      LOG.warn(String.format("Beginning phase %d (creating consumer #%d)", phase, phase));

      Source.Context sourceContext = ContextInfoCreator.createSourceContext(
          "s",
          false,
          OnRecordError.TO_ERROR,
          ImmutableList.of("a")
      );

      final int numProduceBatches = 2 * numPartitions * (phase + 1);
      LOG.warn(String.format(
          "Producing %d batches of %d messages on topic %s",
          numProduceBatches,
          KafkaConsumer09IT.NUM_MESSAGES,
          topic
      ));
      for (int j = 0; j < numProduceBatches; j++) {
        produce(topic, "localhost:" + port, message, j % numPartitions);
      }

      consumers.add(createKafkaConsumer(port, topic, sourceContext));

      // allow rebalance to happen
      final int rebalanceSleep = 5000;
      KafkaConsumer09IT.LOG.warn(String.format("Sleeping %d ms to allow rebalance to happen", rebalanceSleep));
      ThreadUtil.sleep(rebalanceSleep);

      for (int c = 0; c<consumers.size(); c++) {
        SdcKafkaConsumer consumer = consumers.get(c);

        final int messagesToReadPerConsumer = KafkaConsumer09IT.NUM_MESSAGES * numProduceBatches / consumers.size();
        //final int messagesToReadPerConsumer = NUM_MESSAGES * numPartitions / consumers.size();
        KafkaConsumer09IT.LOG.warn(String.format(
            "Attempting to read %d messages from consumer %d (with assignments %s) in phase %d",
            messagesToReadPerConsumer,
            c,
            ((BaseKafkaConsumer09)consumer).getCurrentAssignments(),
            phase
        ));
        final List<MessageAndOffset> read = new LinkedList<>();

        int sleepNumber = 0;

        int numRead = 0;
        while (numRead < messagesToReadPerConsumer) {
          MessageAndOffset next = consumer.read();
          if (next == null) {
            if (sleepNumber++ > KafkaConsumer09IT.MAX_NUM_SLEEPS) {
              KafkaConsumer09IT.LOG.warn(String.format("Giving up after %d sleeps", sleepNumber));
              break;
            } else {
              final int sleepInterval = 1000;
              KafkaConsumer09IT.LOG.warn(String.format("Sleeping for %d ms waiting for messages", sleepInterval));
              ThreadUtil.sleep(sleepInterval);
            }
          } else {
            read.add(next);
            numRead++;
          }
        }
        Assert.assertEquals(messagesToReadPerConsumer, read.size());
      }

      KafkaConsumer09IT.LOG.warn(String.format("Checking commit partitions at end of phase %d", phase));

      Set<Integer> partitions = new HashSet<>();
      for (int c = 0; c < consumers.size(); c++) {
        SdcKafkaConsumer consumer = consumers.get(c);
        final Set<Map.Entry<TopicPartition, OffsetAndMetadata>> offsets = ((BaseKafkaConsumer09) consumer)
            .getTopicPartitionToOffsetMetadataMap()
            .entrySet();

        KafkaConsumer09IT.LOG.warn(String.format("Consumer %d is about to commit %d partitions", c, offsets.size()));
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry :
            offsets) {
          final int partition = entry.getKey().partition();
          KafkaConsumer09IT.LOG.warn(String.format("Consumer %d is about to partitions %d", c, partition));
          if (!partitions.add(partition)) {
            Assert.fail(String.format("Partition %d was committed by more than one consumer", partition));
          }
        }
        // commit clears the topicPartitionToOffsetMetadataMap, so do that checking above before calling it
        consumer.commit();
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
  protected SdcKafkaConsumer createKafkaConsumer(int port, String topic, Source.Context sourceContext) throws
      StageException {
    Map<String, Object> props = new HashMap<>();
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "earliest");
    props.put("session.timeout.ms", "30000");
    props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
    props.put(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    SdcKafkaConsumer sdcKafkaConsumer = createSdcKafkaConsumer(
        "localhost:" + port,
        topic,
        1000,
        sourceContext,
        props,
        CONSUMER_GROUP_NAME
    );
    sdcKafkaConsumer.validate(new ArrayList<Stage.ConfigIssue>(), sourceContext);
    sdcKafkaConsumer.init();
    return sdcKafkaConsumer;
  }

  protected void produce(String topic, String bootstrapServers, String message) {
    produce(topic, bootstrapServers, message, null);
  }

  protected void produce(String topic, String bootstrapServers, String message, Integer partition) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put(KafkaConstants.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(KafkaConstants.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(props);
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

  protected SdcKafkaConsumer createSdcKafkaConsumer(
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
        group,
        100,
        false,
        KafkaAutoOffsetReset.EARLIEST.name().toLowerCase(),
        0
    );
    SdcKafkaConsumerFactory sdcKafkaConsumerFactory = SdcKafkaConsumerFactory.create(settings);
    return sdcKafkaConsumerFactory.create();
  }
}
