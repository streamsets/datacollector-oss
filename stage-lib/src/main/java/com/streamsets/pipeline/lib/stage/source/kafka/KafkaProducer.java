/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

  private static final String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
  private static final String KEY_SERIALIZER_CLASS_KEY = "key.serializer.class";
  private static final String PRODUCER_TYPE_KEY = "producer.type";
  private static final String PRODUCER_TYPE_DEFAULT = "sync";
  private static final String SERIALIZER_CLASS_KEY = "serializer.class";
  private static final String REQUEST_REQUIRED_ACKS_KEY = "request.required.acks";
  private static final String REQUEST_REQUIRED_ACKS_DEFAULT = "1";
  private static final String DEFAULT_ENCODER_CLASS = "kafka.serializer.DefaultEncoder";
  private static final String STRING_ENCODER_CLASS = "kafka.serializer.StringEncoder";
  private static final String PARTITIONER_CLASS_KEY = "partitioner.class";
  private static final String RANDOM_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.stage.source.kafka.RandomPartitioner";
  private static final String ROUND_ROBIN_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.stage.source.kafka.RoundRobinPartitioner";
  private static final String FIXED_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.stage.source.kafka.FixedPartitioner";

  /*Topic to readData from*/
  private final String topic;
  /*Topic to readData from*/
  private final String partitionKey;
  /*Host on which the seed broker is running*/
  private final KafkaBroker broker;

  private final PayloadType payloadType;
  private final PartitionStrategy partitionStrategy;

  private List<KeyedMessage<String, byte[]>> messageList;
  private Producer<String, byte[]> producer;

  public KafkaProducer(String topic, String partitionKey, KafkaBroker broker, PayloadType payloadType,
                       PartitionStrategy partitionStrategy) {
    this.topic = topic;
    this.partitionKey = partitionKey;
    this.broker = broker;
    this.payloadType = payloadType;
    this.partitionStrategy = partitionStrategy;
    this.messageList = new ArrayList<>();
  }

  public void init() {
    Properties props = new Properties();
    props.put(METADATA_BROKER_LIST_KEY, this.broker.getHost() + ":" + this.broker.getPort());
    props.put(PRODUCER_TYPE_KEY, PRODUCER_TYPE_DEFAULT);
    props.put(REQUEST_REQUIRED_ACKS_KEY, REQUEST_REQUIRED_ACKS_DEFAULT);
    props.put(KEY_SERIALIZER_CLASS_KEY, STRING_ENCODER_CLASS);
    configurePartitionStrategy(props, partitionStrategy);
    configureSerializer(props, payloadType);
    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<>(config);
  }

  public void enqueueMessage(byte[] message) throws IOException {
    messageList.add(new KeyedMessage<>(topic, partitionKey, message));
  }

  public void write() {
    producer.send(messageList);
  }

  private void configureSerializer(Properties props, PayloadType payloadType) {
    if(payloadType == PayloadType.LOG) {
      props.put(SERIALIZER_CLASS_KEY, DEFAULT_ENCODER_CLASS);
    }
  }

  private void configurePartitionStrategy(Properties props, PartitionStrategy partitionStrategy) {
    if (partitionStrategy == PartitionStrategy.RANDOM) {
      props.put(PARTITIONER_CLASS_KEY, RANDOM_PARTITIONER_CLASS);
    } else if (partitionStrategy == PartitionStrategy.ROUND_ROBIN) {
      props.put(PARTITIONER_CLASS_KEY, ROUND_ROBIN_PARTITIONER_CLASS);
    } else if (partitionStrategy == PartitionStrategy.FIXED) {
      props.put(PARTITIONER_CLASS_KEY, FIXED_PARTITIONER_CLASS);
    }
  }

  public void destroy() {
    if(producer != null) {
      producer.close();
    }
  }
}
