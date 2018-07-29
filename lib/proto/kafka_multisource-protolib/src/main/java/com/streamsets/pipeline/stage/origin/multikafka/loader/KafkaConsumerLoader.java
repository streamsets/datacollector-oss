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
package com.streamsets.pipeline.stage.origin.multikafka.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.multikafka.MultiSdcKafkaConsumer;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

public abstract class KafkaConsumerLoader {

  /**
   * Instantiated KafkaConsumer Loader (it will be different version based on kafka version that is being used).
   */
  private static KafkaConsumerLoader delegate;

  /**
   * Since the test delegate lives in different scope (test one), we load it as usual, but thread it differently.
   */
  private static KafkaConsumerLoader testDelegate;
  private static final String TEST_DELEGATE_NAME = "com.streamsets.pipeline.stage.origin.multikafka.loader.MockKafkaConsumerLoader";

  @VisibleForTesting
  public static boolean isTest = false;

  static {
    int count = 0;

    Set<String> loaderClasses = new HashSet<>();
    for(KafkaConsumerLoader loader : ServiceLoader.load(KafkaConsumerLoader.class)) {
      String loaderName = loader.getClass().getName();
      loaderClasses.add(loaderName);

      if(TEST_DELEGATE_NAME.equals(loaderName)) {
        testDelegate = loader;
      } else {
        count++;
        delegate = loader;
      }
    }

    if(count > 1) {
      throw new RuntimeException(Utils.format(
        "Unexpected number of loaders, found {} instead of 1: {}",
        count,
        StringUtils.join(loaderClasses, ", ")
      ));
    }
  }

  protected abstract MultiSdcKafkaConsumer createConsumerInternal(Properties properties);

  public static MultiSdcKafkaConsumer createConsumer(Properties properties) {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      return testDelegate.createConsumerInternal(properties);
    } else {
      Preconditions.checkNotNull(delegate);
      return delegate.createConsumerInternal(properties);
    }
  }

}
