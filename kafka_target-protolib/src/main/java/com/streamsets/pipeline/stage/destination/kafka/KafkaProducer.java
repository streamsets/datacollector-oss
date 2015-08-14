/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.kafka;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.KafkaConnectionException;
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
  private static final String RANDOM_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.RandomPartitioner";
  private static final String ROUND_ROBIN_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.RoundRobinPartitioner";
  private static final String EXPRESSION_PARTITIONER_CLASS = "com.streamsets.pipeline.lib.ExpressionPartitioner";

  /*Topic to readData from*/
  /*Host on which the seed broker is running*/
  private final String metadataBrokerList;
  private final Map<String, String> kafkaProducerConfigs;
  private final DataFormat producerPayloadType;
  private final PartitionStrategy partitionStrategy;
  private List<KeyedMessage<String, byte[]>> messageList;
  private Producer<String, byte[]> producer;

  public KafkaProducer(String metadataBrokerList, DataFormat producerPayloadType,
                       PartitionStrategy partitionStrategy, Map<String, String> kafkaProducerConfigs) {
    this.metadataBrokerList = metadataBrokerList;
    this.producerPayloadType = producerPayloadType;
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
    configureSerializer(props, producerPayloadType);
    //request.required.acks
    props.put(REQUEST_REQUIRED_ACKS_KEY, REQUEST_REQUIRED_ACKS_DEFAULT);

    addUserConfiguredProperties(props);

    ProducerConfig config = new ProducerConfig(props);
    producer = new Producer<>(config);
  }

  public void destroy() {
    if(producer != null) {
      producer.close();
    }
  }

  public void enqueueMessage(String topic, byte[] message, String partitionKey) {
    //Topic could be a record EL string. This is not a good place to evaluate expression
    //Hence get topic as parameter
    messageList.add(new KeyedMessage<>(topic, partitionKey, message));
  }

  public List<KeyedMessage<String, byte[]>> getMessageList() {
    return messageList;
  }

  public void write() throws StageException {
    try {
      producer.send(messageList);
      messageList.clear();
    } catch (Exception e) {
      //Producer internally refreshes metadata and retries if there is any recoverable exception.
      //If retry fails, a FailedToSendMessageException is thrown.
      //In this case we want to fail pipeline.
      LOG.error(KafkaErrors.KAFKA_50.getMessage(), e.toString(), e);
      throw new KafkaConnectionException(KafkaErrors.KAFKA_50, e.toString(), e);
    }
  }


  private void configureSerializer(Properties props, DataFormat producerPayloadType) {
    if(producerPayloadType == DataFormat.TEXT) {
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
