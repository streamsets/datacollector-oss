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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class FactoriesBean {

  private static final Logger LOG = LoggerFactory.getLogger(FactoriesBean.class);

  public abstract SdcKafkaProducerFactory createSdcKafkaProducerFactory();

  public abstract SdcKafkaValidationUtilFactory createSdcKafkaValidationUtilFactory();

  public abstract SdcKafkaConsumerFactory createSdcKafkaConsumerFactory();

  public abstract SdcKafkaLowLevelConsumerFactory createSdcKafkaLowLevelConsumerFactory();

  private static ServiceLoader<FactoriesBean> factoriesBeanLoader = ServiceLoader.load(FactoriesBean.class);

  private static FactoriesBean factoriesBean;

  private final static Map<String, String[]> classHierarchy = ImmutableMap.of(
      "com.streamsets.pipeline.kafka.impl.Kafka09FactoriesBean",
      new String[] {
        "com.streamsets.pipeline.kafka.impl.Kafka10FactoriesBean",
        "com.streamsets.pipeline.kafka.impl.Kafka11FactoriesBean",
        "com.streamsets.pipeline.kafka.impl.MapR52Streams09FactoriesBean",
        "com.streamsets.pipeline.kafka.impl.MapR61Streams11FactoriesBean"
      },
      "com.streamsets.pipeline.kafka.impl.Kafka11FactoriesBean",
      new String[] {
        "com.streamsets.pipeline.kafka.impl.Kafka1_0FactoriesBean",
        "com.streamsets.pipeline.kafka.impl.Kafka20FactoriesBean"
      },
      "com.streamsets.pipeline.kafka.impl.MapRStreams09FactoriesBean",
      new String[] {
        "com.streamsets.pipeline.kafka.impl.MapR52Streams09FactoriesBean",
      }
  );

  private static boolean isSubclass(String bean, String superclass) {
    String[] subclasses = classHierarchy.get(superclass);
    if (subclasses == null) {
      return false;
    }
    for (String subclass : subclasses) {
      if (subclass.equals(bean) || isSubclass(bean, subclass)) {
        return true;
      }
    }
    return false;
  }

  static {
    factoriesBean = loadBean(StreamSupport.stream(factoriesBeanLoader.spliterator(), false).collect(Collectors.toList()));
  }

  @VisibleForTesting
  static FactoriesBean loadBean(List<FactoriesBean> beans) {
    Set<String> loadedBeans = new HashSet<>();
    FactoriesBean factoriesBean = null;

    for (FactoriesBean bean: beans) {
      String beanName = bean.getClassName();
      LOG.info("Found FactoriesBean loader {}", beanName);

      boolean subclass = false;
      if (factoriesBean == null || isSubclass(beanName, factoriesBean.getClassName())) {
        if (factoriesBean != null) {
          loadedBeans.remove(factoriesBean.getClassName());
        }
        factoriesBean = bean;
        subclass = true;
      }
      if (subclass || !isSubclass(factoriesBean.getClassName(), beanName)) {
        loadedBeans.add(beanName);
      }
    }
    if (loadedBeans.size() > 1) {
      throw new RuntimeException(Utils.format("Unexpected number of loaders, found {} instead of 1: {}",
        loadedBeans.size(),
        StringUtils.join(loadedBeans, ", ")
      ));
    }

    return factoriesBean;
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

  public String getClassName() {
    return getClass().getName();
  }
}
