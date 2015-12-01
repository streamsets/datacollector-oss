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


import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducer09 implements SdcKafkaProducer {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer09.class);

  private static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  private static final String KEY_SERIALIZER_KEY = "key.serializer";
  private static final String KEY_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer";
  private static final String VALUE_SERIALIZER_KEY = "value.serializer";
  private static final String VALUE_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String REQUEST_REQUIRED_ACKS_KEY = "acks";
  private static final String REQUEST_REQUIRED_ACKS_DEFAULT = "1";

  private static final String PARTITIONER_CLASS_KEY = "partitioner.class";
  private static final String RANDOM_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.RandomPartitioner";
  private static final String ROUND_ROBIN_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.RoundRobinPartitioner";
  private static final String EXPRESSION_PARTITIONER_CLASS = "com.streamsets.pipeline.kafka.impl.ExpressionPartitioner";

  private final String metadataBrokerList;
  private final Map<String, Object> kafkaProducerConfigs;
  private final PartitionStrategy partitionStrategy;
  private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer;
  private final List<Future<RecordMetadata>> futureList;

  public KafkaProducer09(
      String metadataBrokerList,
      Map<String, Object> kafkaProducerConfigs,
      PartitionStrategy partitionStrategy
  ) {
    this.metadataBrokerList = metadataBrokerList;
    this.kafkaProducerConfigs = kafkaProducerConfigs;
    this.partitionStrategy = partitionStrategy;
    this.futureList = new ArrayList<>();
  }

  @Override
  public void init() throws StageException {
    Properties props = new Properties();

    // bootstrap servers
    props.put(BOOTSTRAP_SERVERS_KEY, metadataBrokerList);

    // key and value serializers
    props.put(KEY_SERIALIZER_KEY, KEY_SERIALIZER_DEFAULT);
    props.put(VALUE_SERIALIZER_KEY, VALUE_SERIALIZER_DEFAULT);

    // request.required.acks
    props.put(REQUEST_REQUIRED_ACKS_KEY, REQUEST_REQUIRED_ACKS_DEFAULT);

    // partitioner.class
    configurePartitionStrategy(props, partitionStrategy);

    addUserConfiguredProperties(props);

    producer = new KafkaProducer<>(props);
  }

  @Override
  public void destroy() {
    if(producer != null) {
      producer.close();
    }
  }

  @Override
  public void enqueueMessage(String topic, byte[] message, String partitionKey) {
    ProducerRecord<String, byte[]> e = new ProducerRecord<>(topic, partitionKey, message);
    // send will place this record in the buffer to be batched later
    futureList.add(producer.send(e));
  }

  @Override
  public void write() throws StageException {
    // force all records in the buffer to be written out
    producer.flush();
    // make sure each record was written and handle exception if any
    for(Future<RecordMetadata> f : futureList) {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        // error writing this record to kafka broker.
        LOG.error(KafkaErrors.KAFKA_50.getMessage(), e.toString(), e);
        // throwing of this exception results in stopped pipeline as it is not handled by KafkaTarget
        // Retry feature at the pipeline level will re attempt
        throw new StageException(KafkaErrors.KAFKA_50, e.toString(), e);
      }
    }
    futureList.clear();
  }

  @Override
  public void clearMessages() {
    futureList.clear();
  }

  @Override
  public String getVersion() {
    return Kafka09Constants.KAFKA_VERSION;
  }

  private void addUserConfiguredProperties(Properties props) {
    //The following options, if specified, are ignored : "bootstrap.servers"
    if (kafkaProducerConfigs != null && !kafkaProducerConfigs.isEmpty()) {
      kafkaProducerConfigs.remove(BOOTSTRAP_SERVERS_KEY);
      for (Map.Entry<String, Object> producerConfig : kafkaProducerConfigs.entrySet()) {
        props.put(producerConfig.getKey(), producerConfig.getValue());
      }
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
      // org.apache.kafka.clients.producer.internals.DefaultPartitioner
    }
  }

}