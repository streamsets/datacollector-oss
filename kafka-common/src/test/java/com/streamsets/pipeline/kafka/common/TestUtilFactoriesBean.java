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
package com.streamsets.pipeline.kafka.common;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

@Category(SingleForkNoReuseTest.class)
public abstract class TestUtilFactoriesBean {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtilFactoriesBean.class);

  public abstract SdcKafkaTestUtilFactory createSdcKafkaTestUtilFactory();

  private static ServiceLoader<TestUtilFactoriesBean> factoriesBeanLoader;

  private static TestUtilFactoriesBean testUtilFactoriesBean;

  static {
    LOG.info("Loading TestUtilFactoriesBean");
    factoriesBeanLoader = ServiceLoader.load(TestUtilFactoriesBean.class);
    int serviceCount = 0;
    for (TestUtilFactoriesBean bean : factoriesBeanLoader) {
      testUtilFactoriesBean = bean;
      serviceCount++;
    }
    if (serviceCount != 1) {
      throw new RuntimeException(Utils.format("Unexpected number of TestUtilFactoriesBean: {} instead of 1", serviceCount));
    }

    LOG.info("Loaded TestUtilFactoriesBean: {}", testUtilFactoriesBean.getClass().getName());
  }

  public static SdcKafkaTestUtilFactory getSdcKafkaTestUtilFactory() {
    return testUtilFactoriesBean.createSdcKafkaTestUtilFactory();
  }
}
