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
package com.streamsets.pipeline.stage.origin.tokafka;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
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

  public SdcKafkaProducerPooledObjectFactory(KafkaTargetConfig kafkaTargetConfig, DataFormat dataFormat) {
    LOG.debug("Kafka producer config: brokers '{}' configs '{}'",
        kafkaTargetConfig.connectionConfig.connection.metadataBrokerList,
        kafkaTargetConfig.kafkaProducerConfigs
    );
    settings = new ProducerFactorySettings(kafkaTargetConfig.kafkaProducerConfigs == null
        ? Collections.<String, Object>emptyMap() : new HashMap<String, Object>(kafkaTargetConfig.kafkaProducerConfigs),
        PartitionStrategy.ROUND_ROBIN, kafkaTargetConfig.connectionConfig.connection.metadataBrokerList, dataFormat, false
    );
  }

  @Override
  public PooledObject<SdcKafkaProducer> wrap(SdcKafkaProducer producer) {
    return new DefaultPooledObject<>(producer);
  }

  SdcKafkaProducer createInternal() {
    return SdcKafkaProducerFactory.create(settings).create();
  }

  @Override
  public SdcKafkaProducer create() throws Exception {
    LOG.debug("Creating Kafka producer");
    SdcKafkaProducer producer = createInternal();
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
