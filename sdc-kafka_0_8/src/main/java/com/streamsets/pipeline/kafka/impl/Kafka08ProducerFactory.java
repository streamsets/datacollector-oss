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

import com.streamsets.pipeline.kafka.api.ProducerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;

public class Kafka08ProducerFactory extends SdcKafkaProducerFactory {

  private ProducerFactorySettings settings;

  public Kafka08ProducerFactory() {
  }

  @Override
  protected void init(ProducerFactorySettings settings) {
    this.settings = settings;
  }

  @Override
  public SdcKafkaProducer create() {
    return new KafkaProducer08(
        settings.getMetadataBrokerList(),
        settings.getDataFormat(),
        settings.getPartitionStrategy(),
        settings.getKafkaProducerConfigs()
    );
  }
}
