/**
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

import com.streamsets.pipeline.api.impl.Utils;
import java.util.ServiceLoader;

public abstract class FactoriesBean {

  public abstract SdcKafkaProducerFactory createSdcKafkaProducerFactory();

  public abstract SdcKafkaValidationUtilFactory createSdcKafkaValidationUtilFactory();

  public abstract SdcKafkaConsumerFactory createSdcKafkaConsumerFactory();

  public abstract SdcKafkaLowLevelConsumerFactory createSdcKafkaLowLevelConsumerFactory();

  private static ServiceLoader<FactoriesBean> factoriesBeanLoader = ServiceLoader.load(FactoriesBean.class);

  private static FactoriesBean factoriesBean;

  private final static String kafka10ClassName = "com.streamsets.pipeline.kafka.impl.Kafka10FactoriesBean";

  static {
    int serviceCount = 0;
    FactoriesBean kafka10FactoriesBean = null;
    for (FactoriesBean bean : factoriesBeanLoader) {
      factoriesBean = bean;
      serviceCount++;
      // Exception for Kafak10 since Kafka10 depends on Kafak09, it should load 2 service loaders
      if(bean.getClass().getName().toString().equals(kafka10ClassName)) {
        kafka10FactoriesBean = bean;
      }
    }

    // Exception for Kafka10 since it should load 2 service loaders
    if(kafka10FactoriesBean != null) {
      factoriesBean = kafka10FactoriesBean;
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
