/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.spark;

import com.streamsets.pipeline.Utils;

import java.util.Properties;
import java.util.ServiceLoader;

public abstract class SparkStreamingBindingFactory {

  private static SparkStreamingBindingFactory bindingFactory;
  private static ServiceLoader<SparkStreamingBindingFactory> factoryLoader = ServiceLoader.load(SparkStreamingBindingFactory.class);

  static {
    int serviceCount = 0;
    for (SparkStreamingBindingFactory factory : factoryLoader) {
      bindingFactory = factory;
      serviceCount++;
    }
    if (serviceCount != 1) {
      throw new RuntimeException(Utils.format("Unexpected number of SparkStreamingFactoryBean: {} instead of 1", serviceCount));
    }
  }
  public static SparkStreamingBinding build(Properties properties){
    return bindingFactory.create(properties);
  }

  public abstract SparkStreamingBinding create(Properties properties);

}
