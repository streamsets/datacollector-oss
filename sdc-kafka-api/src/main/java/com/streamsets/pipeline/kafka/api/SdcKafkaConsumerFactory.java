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
package com.streamsets.pipeline.kafka.api;

public abstract class SdcKafkaConsumerFactory {

  public static SdcKafkaConsumerFactory create(ConsumerFactorySettings settings) {
    SdcKafkaConsumerFactory kafkaConsumerFactory = FactoriesBean.getKafkaConsumerFactory();
    kafkaConsumerFactory.init(settings);
    return kafkaConsumerFactory;
  }

  protected abstract void init(ConsumerFactorySettings settings);

  public abstract SdcKafkaConsumer create();

}
