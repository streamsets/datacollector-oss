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

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.kafka.api.LowLevelConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaLowLevelConsumerFactory;


public class Kafka08LowLevelConsumerFactory extends SdcKafkaLowLevelConsumerFactory {

  private LowLevelConsumerFactorySettings settings;

  @Override
  protected void init(LowLevelConsumerFactorySettings settings) {
    this.settings = settings;
  }

  @Override
  public SdcKafkaLowLevelConsumer create() {
    return new KafkaLowLevelConsumer08(
        settings.getTopic(),
        settings.getPartition(),
        HostAndPort.fromParts(settings.getBrokerHost(), settings.getBrokerPort()),
        settings.getMinFetchSize(),
        settings.getMaxFetchSize(),
        settings.getMaxWaitTime(),
        settings.getClientName()
    );
  }
}
