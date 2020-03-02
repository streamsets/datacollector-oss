/*
 * Copyright 2019 StreamSets Inc.
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
import org.junit.Assert;
import org.junit.Test;


public class TestKafka2_0FactoriesBean {

  @Test
  public void testKafka20FactoriesBean() {
    Assert.assertTrue(FactoriesBean.getKafkaConsumerFactory() instanceof Kafka20ConsumerFactory);
    Assert.assertTrue(FactoriesBean.getKafkaProducerFactory() instanceof Kafka20ProducerFactory);
    Assert.assertTrue(FactoriesBean.getKafkaLowLevelConsumerFactory() instanceof Kafka09LowLevelConsumerFactory);
    Assert.assertTrue(FactoriesBean.getKafkaValidationUtilFactory() instanceof Kafka11ValidationUtilFactory);
  }
}
