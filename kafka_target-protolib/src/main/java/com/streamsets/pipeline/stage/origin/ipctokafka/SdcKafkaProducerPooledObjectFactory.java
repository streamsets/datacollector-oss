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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;

public class SdcKafkaProducerPooledObjectFactory extends BasePooledObjectFactory<SdcKafkaProducer> {
  private static final Logger LOG = LoggerFactory.getLogger(SdcKafkaProducerPooledObjectFactory.class);
  ProducerFactorySettings settings;

  public SdcKafkaProducerPooledObjectFactory(RpcConfigs configs, KafkaConfigBean kafkaConfigBean) {
    LOG.debug("Kafka producer config: brokers '{}' configs '{}'",
        kafkaConfigBean.kafkaConfig.metadataBrokerList,
        kafkaConfigBean.kafkaConfig.kafkaProducerConfigs
    );
    settings = new ProducerFactorySettings(kafkaConfigBean.kafkaConfig.kafkaProducerConfigs == null
        ? Collections.<String, Object>emptyMap()
        : new HashMap<String, Object>(kafkaConfigBean.kafkaConfig.kafkaProducerConfigs),
        PartitionStrategy.ROUND_ROBIN,
      kafkaConfigBean.kafkaConfig.metadataBrokerList,
        DataFormat.SDC_JSON
    );
  }

  @Override
  public PooledObject<SdcKafkaProducer> wrap(SdcKafkaProducer producer) {
    return new DefaultPooledObject<>(producer);
  }

  @Override
  public SdcKafkaProducer create() throws Exception {
    LOG.debug("Creating Kafka producer");
    SdcKafkaProducer producer = SdcKafkaProducerFactory.create(settings).create();
    producer.init();
    LOG.debug("Creating Kafka producer '{}'", producer);
    return producer;
  }

  @Override
  public void activateObject(PooledObject<SdcKafkaProducer> p) throws Exception {
    LOG.debug("Activating Kafka producer '{}'", p.getObject());
  }

  @Override
  public void passivateObject(PooledObject<SdcKafkaProducer> p) throws Exception {
    LOG.debug("Deactivating Kafka producer '{}'", p.getObject());
    p.getObject().clearMessages();
  }

  @Override
  public void destroyObject(PooledObject<SdcKafkaProducer> p) throws Exception {
    LOG.debug("Destroying Kafka producer '{}'", p.getObject());
    p.getObject().destroy();
  }

}
