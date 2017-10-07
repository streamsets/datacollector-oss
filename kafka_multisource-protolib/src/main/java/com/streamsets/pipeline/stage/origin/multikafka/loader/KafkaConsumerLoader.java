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

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.origin.multikafka.MultiSdcKafkaConsumer;

import java.util.Properties;
import java.util.ServiceLoader;

public abstract class KafkaConsumerLoader {

  /**
   * Instantiated KafkaConsumer Loader (it will be different version based on kafka version that is being used).
   */
  private static KafkaConsumerLoader delegate;
  static {
    int count = 0;

    for(KafkaConsumerLoader loader : ServiceLoader.load(KafkaConsumerLoader.class)) {
      count++;
      delegate = loader;
    }

    if(count != 1) {
      throw new RuntimeException(Utils.format("Unexpected number of loaders ({} instead of 1)", count));
    }
  }

  protected abstract MultiSdcKafkaConsumer createConsumerInternal(Properties properties);

  public static MultiSdcKafkaConsumer createConsumer(Properties properties) {
    return delegate.createConsumerInternal(properties);
  }

}
