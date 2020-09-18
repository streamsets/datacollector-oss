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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import com.streamsets.pipeline.stage.origin.multikafka.MultiSdcKafkaConsumer;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  private static final String
      TEST_DELEGATE_NAME
      = "com.streamsets.pipeline.stage.origin.multikafka.loader.MockKafkaConsumerLoader";

  @VisibleForTesting
  public static boolean isTest = false;

  private static ServiceLoader<KafkaConsumerLoader> kafkaConsumerServiceLoader = ServiceLoader.load(KafkaConsumerLoader.class);

  private static final Map<String, String[]> loaderHierarchy = ImmutableMap.of(
      "com.streamsets.pipeline.stage.origin.multikafka.v0_10.loader.Kafka0_10ConsumerLoader",
      new String[] {"com.streamsets.pipeline.stage.origin.multikafka.v0_11.loader.Kafka0_11ConsumerLoader"}
  );

  private static boolean isSubclass(String loader, String parent) {
    String[] children = loaderHierarchy.get(parent);
    if (children == null) {
      return false;
    }
    for (String child : children) {
      if (child.equals(loader) || isSubclass(loader, child)) {
        return true;
      }
    }
    return false;
  }

  static {
    int count = 0;

    Set<String> loaderClasses = new HashSet<>();
    for (KafkaConsumerLoader loader : kafkaConsumerServiceLoader) {
      String loaderName = loader.getClass().getName();

      if (TEST_DELEGATE_NAME.equals(loaderName)) {
        testDelegate = loader;
      } else {
        boolean subclass = false;
        if (delegate == null || isSubclass(loaderName, delegate.getClass().getName())) {
          if (delegate != null) {
            loaderClasses.remove(delegate.getClass().getName());
            count--;
          }
          delegate = loader;
          subclass = true;
        }
        if (subclass || !isSubclass(delegate.getClass().getName(), loaderName)) {
          loaderClasses.add(loaderName);
          count++;
        }
      }
    }

    if (count > 1) {
      throw new RuntimeException(Utils.format("Unexpected number of loaders, found {} instead of 1: {}",
          count,
          StringUtils.join(loaderClasses, ", ")
      ));
    }
  }

  protected abstract void validateConsumerConfiguration(
      Properties properties,
      Stage.Context context,
      KafkaAutoOffsetReset kafkaAutoOffsetReset,
      long timestampToSearchOffsets,
      List<String> topicsList
  );

  protected abstract MultiSdcKafkaConsumer<String, byte[]> createConsumerInternal(Properties properties);

  public static MultiSdcKafkaConsumer<String, byte[]> createConsumer(
      Properties properties,
      Stage.Context context,
      KafkaAutoOffsetReset kafkaAutoOffsetReset,
      long timestampToSearchOffsets,
      List<String> topics
  ) {
    if (isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.validateConsumerConfiguration(properties,
          context,
          kafkaAutoOffsetReset,
          timestampToSearchOffsets,
          topics
      );
      return testDelegate.createConsumerInternal(properties);
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.validateConsumerConfiguration(properties,
          context,
          kafkaAutoOffsetReset,
          timestampToSearchOffsets,
          topics
      );
      return delegate.createConsumerInternal(properties);
    }
  }

}
