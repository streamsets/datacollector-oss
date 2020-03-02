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

import com.streamsets.pipeline.kafka.api.FactoriesBean;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;

public class Kafka1_0FactoriesBean extends FactoriesBean {

  @Override
  public SdcKafkaProducerFactory createSdcKafkaProducerFactory() {
    return new Kafka09ProducerFactory();
  }

  @Override
  public SdcKafkaValidationUtilFactory createSdcKafkaValidationUtilFactory() {
    return new Kafka11ValidationUtilFactory();
  }

  @Override
  public SdcKafkaConsumerFactory createSdcKafkaConsumerFactory() {
    return new Kafka1_0ConsumerFactory();
  }

  @Override
  public SdcKafkaLowLevelConsumerFactory createSdcKafkaLowLevelConsumerFactory() {
    return new Kafka09LowLevelConsumerFactory();
  }

}
