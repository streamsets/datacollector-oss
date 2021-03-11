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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.kafka.exception.KafkaConnectionException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaProducer08 implements SdcKafkaProducer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer08.class);

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
  private static final String RANDOM_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.RandomPartitioner";
  private static final String ROUND_ROBIN_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.RoundRobinPartitioner";
  private static final String EXPRESSION_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.ExpressionPartitioner";

  /*Topic to readData from*/
  /*Host on which the seed broker is running*/
  private final String metadataBrokerList;
  private final Map<String, Object> kafkaProducerConfigs;
  private final DataFormat producerPayloadType;
  private final PartitionStrategy partitionStrategy;
  private List<KeyedMessage> messageList;
  private Producer producer;

  public KafkaProducer08(
      String metadataBrokerList,
      DataFormat producerPayloadType,
      PartitionStrategy partitionStrategy,
      Map<String, Object> kafkaProducerConfigs
  ) {
    this.metadataBrokerList = metadataBrokerList;
    this.producerPayloadType = producerPayloadType;
    this.partitionStrategy = partitionStrategy;
    this.messageList = new ArrayList<>();
    this.kafkaProducerConfigs = kafkaProducerConfigs;
  }

  @Override
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

  @Override
  public void destroy() {
    if(producer != null) {
      producer.close();
    }
  }

  @Override
  public String getVersion() {
    return Kafka08Constants.KAFKA_VERSION;
  }

  @Override
  public void enqueueMessage(String topic, Object message, Object messageKey) {
    //Topic could be a record EL string. This is not a good place to evaluate expression
    //Hence get topic as parameter
    messageList.add(new KeyedMessage<>(topic, messageKey, message));
  }

  @Override
  public void clearMessages() {
    messageList.clear();
  }

  @Override
  public List<Record> write(Stage.Context context) throws StageException {
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
    return Collections.emptyList();
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
    } else if (partitionStrategy == PartitionStrategy.DEFAULT) {
      //default partitioner class
    }
  }

  private void addUserConfiguredProperties(Properties props) {
    //The following options, if specified, are ignored : "metadata.broker.list", "producer.type",
    // "key.serializer.class", "partitioner.class", "serializer.class".
    if (kafkaProducerConfigs != null && !kafkaProducerConfigs.isEmpty()) {
      kafkaProducerConfigs.remove(METADATA_BROKER_LIST_KEY);
      kafkaProducerConfigs.remove(KEY_SERIALIZER_CLASS_KEY);
      kafkaProducerConfigs.remove(SERIALIZER_CLASS_KEY);

      for (Map.Entry<String, Object> producerConfig : kafkaProducerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
    }
  }
}
