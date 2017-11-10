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

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;
import java.util.Set;

public abstract class FactoriesBean {

  private static final Logger LOG = LoggerFactory.getLogger(FactoriesBean.class);

  public abstract SdcKafkaProducerFactory createSdcKafkaProducerFactory();

  public abstract SdcKafkaValidationUtilFactory createSdcKafkaValidationUtilFactory();

  public abstract SdcKafkaConsumerFactory createSdcKafkaConsumerFactory();

  public abstract SdcKafkaLowLevelConsumerFactory createSdcKafkaLowLevelConsumerFactory();

  private static ServiceLoader<FactoriesBean> factoriesBeanLoader = ServiceLoader.load(FactoriesBean.class);

  private static FactoriesBean factoriesBean;

  private final static Set<String> subclassNames = ImmutableSet.of(
    "com.streamsets.pipeline.kafka.impl.Kafka10FactoriesBean",
    "com.streamsets.pipeline.kafka.impl.Kafka11FactoriesBean",
    "com.streamsets.pipeline.kafka.impl.Kafka1_0FactoriesBean"
  );

  static {
    int serviceCount = 0;
    FactoriesBean subclassBean = null;
    for (FactoriesBean bean : factoriesBeanLoader) {
      LOG.info("Found FactoriesBean loader {}", bean.getClass().getName());
      factoriesBean = bean;
      serviceCount++;

      if(subclassNames.contains(bean.getClass().getName())) {
        if(subclassBean != null) {
          throw new RuntimeException(Utils.format("More then one subclass beans found: {}, {}", subclassBean, bean.getClass().getName()));
        }
        subclassBean = bean;
      }
    }

    if(subclassBean != null) {
      factoriesBean = subclassBean;
      serviceCount--;
    }

    if (serviceCount != 1) {
      throw new RuntimeException(Utils.format("Unexpected number of FactoriesBean: {} instead of 1", serviceCount));
    }
  }

  public static SdcKafkaProducerFactory getKafkaProducerFactory() {
    return factoriesBean.createSdcKafkaProducerFactory();
  }

  public static SdcKafkaValidationUtilFactory getKafkaValidationUtilFactory() {
    return factoriesBean.createSdcKafkaValidationUtilFactory();
  }

  public static SdcKafkaConsumerFactory getKafkaConsumerFactory() {
    return factoriesBean.createSdcKafkaConsumerFactory();
  }

  public static SdcKafkaLowLevelConsumerFactory getKafkaLowLevelConsumerFactory() {
    return factoriesBean.createSdcKafkaLowLevelConsumerFactory();
  }
}
