/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.kafka.impl;

import com.streamsets.pipeline.api.Source;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer10 extends KafkaConsumer09{
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer10.class);
  public KafkaConsumer10(
      String bootStrapServers,
      String topic,
      String consumerGroup,
      Map<String, Object> kafkaConsumerConfigs,
      Source.Context context
  ) {
    super(bootStrapServers, topic, consumerGroup, kafkaConsumerConfigs, context);
  }

  protected void createConsumer() {
    Properties kafkaConsumerProperties = new Properties();
    configureKafkaProperties(kafkaConsumerProperties);
    LOG.debug("Creating Kafka Consumer with properties {}" , kafkaConsumerProperties.toString());
    kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
    Collection<String> collection = Collections.singletonList(topic);
    kafkaConsumer.subscribe(collection);
  }

}
