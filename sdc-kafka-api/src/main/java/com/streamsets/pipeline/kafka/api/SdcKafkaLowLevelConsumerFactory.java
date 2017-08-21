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

public abstract class SdcKafkaLowLevelConsumerFactory {

  public static SdcKafkaLowLevelConsumerFactory create(LowLevelConsumerFactorySettings settings) {
    SdcKafkaLowLevelConsumerFactory factory = FactoriesBean.getKafkaLowLevelConsumerFactory();
    factory.init(settings);
    return factory;
  }

  protected abstract void init(LowLevelConsumerFactorySettings settings);

  public abstract SdcKafkaLowLevelConsumer create();

}
