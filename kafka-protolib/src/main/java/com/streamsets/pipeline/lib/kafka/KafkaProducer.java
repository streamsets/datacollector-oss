/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.kafka;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.KafkaStageLibError;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private static final String RANDOM_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.kafka.RandomPartitioner";
  private static final String ROUND_ROBIN_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.kafka.RoundRobinPartitioner";
  private static final String EXPRESSION_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.kafka.ExpressionPartitioner";
  private static final String COLON = ":";

  private static final int METADATA_READER_TIME_OUT = 10000;
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final String METADATA_READER_CLIENT = "metadataReaderClient";
  private static final int MAX_RECONNECT_ATTEMPTS = 4;


  /*Topic to readData from*/
  private final String topic;
  /*Host on which the seed broker is running*/
  private final String metadataBrokerList;

  private final Map<String, String> kafkaProducerConfigs;

  private final PayloadType payloadType;
  private final PartitionStrategy partitionStrategy;

  private List<KeyedMessage<String, byte[]>> messageList;
  private Producer<String, byte[]> producer;

  private int numberOfPartitions;

  public KafkaProducer(String topic, String metadataBrokerList, PayloadType payloadType,
                       PartitionStrategy partitionStrategy, Map<String, String> kafkaProducerConfigs) {
    this.topic = topic;
    this.metadataBrokerList = metadataBrokerList;
    this.payloadType = payloadType;
    this.partitionStrategy = partitionStrategy;
    this.messageList = new ArrayList<>();
    this.kafkaProducerConfigs = kafkaProducerConfigs;
  }

  public void init() throws StageException {
    Properties props = new Properties();
    //metadata.broker.list
    props.put(METADATA_BROKER_LIST_KEY, metadataBrokerList);
    //producer.type
    props.put(PRODUCER_TYPE_KEY, PRODUCER_TYPE_DEFAULT);
    //key.serializer.class
    props.put(KEY_SERIALIZER_CLASS_KEY, STRING_ENCODER_CLASS);
    //partitioner.class
    configurePartitionStrategy(props, partitionStrategy);
    //serializer.class
    configureSerializer(props, payloadType);
    //request.required.acks
    props.put(REQUEST_REQUIRED_ACKS_KEY, REQUEST_REQUIRED_ACKS_DEFAULT);

    addUserConfiguredProperties(props);

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<>(config);

    numberOfPartitions = KafkaUtil.findNUmberOfPartitions(metadataBrokerList, topic, METADATA_READER_TIME_OUT,
      BUFFER_SIZE, METADATA_READER_CLIENT);
  }

  public void destroy() {
    if(producer != null) {
      producer.close();
    }
  }

  public void enqueueMessage(byte[] message, String partitionKey) {
    messageList.add(new KeyedMessage<>(topic, partitionKey, message));
  }

  public void write() throws StageException {
    int attemptNumber = 0;
    while(true) {
      try {
        attemptNumber++;
        producer.send(messageList);
        //break on successful send
        break;
      } catch (Exception e) {
        reconnect(attemptNumber, e);
      }
    }
    messageList.clear();
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  private void reconnect(int attemptNumber, Exception cause) throws StageException {
    //reconnect and resend
    LOG.warn("Attempting reconnection to kafka broker.");
    if (attemptNumber == MAX_RECONNECT_ATTEMPTS) {
      LOG.error("Reached max reconnect attempts {}. ", MAX_RECONNECT_ATTEMPTS);
      throw new StageException(KafkaStageLibError.KFK_0350, cause.getMessage(), cause);
    }
    try {
      waitBeforeReconnect(attemptNumber);
    } catch (InterruptedException e) {
      LOG.error(KafkaStageLibError.KFK_0350.getMessage(), e.getMessage(), e);
      throw new StageException(KafkaStageLibError.KFK_0350, e.getMessage(), e);
    }
    try {
      destroy();
      init();
    } catch (Exception e) {
      //error connecting
      //no-op. Caller of this function will try again
    }
  }

  private void waitBeforeReconnect(int attemptNumber) throws InterruptedException {
    long timeToWait = 60000;
    switch (attemptNumber) {
      case 1:
        //1 minute
        timeToWait = 60000;
        break;
      case 2:
        //5 minutes
        timeToWait = 300000;
        break;
      case 3:
        //10 minutes
        timeToWait = 600000;
        break;
    }
    LOG.debug("Reconnection attempt {}, waiting {} milli seconds before reconnecting.", attemptNumber, timeToWait);
    Thread.sleep(timeToWait);
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
    } else if (partitionStrategy == PartitionStrategy.EXPRESSION) {
      props.put(PARTITIONER_CLASS_KEY, EXPRESSION_PARTITIONER_CLASS);
    }
  }

  private void addUserConfiguredProperties(Properties props) {
    //The following options, if specified, are ignored : "metadata.broker.list", "producer.type",
    // "key.serializer.class", "partitioner.class", "serializer.class".
    if (kafkaProducerConfigs != null && !kafkaProducerConfigs.isEmpty()) {
      kafkaProducerConfigs.remove(METADATA_BROKER_LIST_KEY);
      kafkaProducerConfigs.remove(PRODUCER_TYPE_KEY);
      kafkaProducerConfigs.remove(KEY_SERIALIZER_CLASS_KEY);
      kafkaProducerConfigs.remove(PARTITIONER_CLASS_KEY);
      kafkaProducerConfigs.remove(SERIALIZER_CLASS_KEY);

      for (Map.Entry<String, String> producerConfig : kafkaProducerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }
}
