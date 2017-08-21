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
package com.streamsets.pipeline.stage.origin.kafka.cluster;

import kafka.utils.TestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

public class TestUtilsBridge {
  private static final String CREATE_BROKER_CONFIG = "createBrokerConfig";
  /**
   * Bridge method for incompatible Kafka TestUtils classes from CDH Kafka <1.3.2
   * @param nodeId
   * @param port
   * @param enableControlledShutdown
   * @param enableDeleteTopic
   * @return Kafka Broker Configuration Properties
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  public static Properties createBrokerConfig(
      int nodeId,
      int port,
      boolean enableControlledShutdown,
      boolean enableDeleteTopic
  ) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    Method method;
    try {
      method = kafka.utils.TestUtils.class.getMethod(CREATE_BROKER_CONFIG, int.class, int.class, boolean.class);
      return (Properties) method.invoke(TestUtils.class, nodeId, port, enableControlledShutdown);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      try {
        method = kafka.utils.TestUtils.class.getMethod(
            CREATE_BROKER_CONFIG,
            int.class,
            int.class,
            boolean.class,
            boolean.class
        );
        return (Properties) method.invoke(TestUtils.class, nodeId, port, enableControlledShutdown, enableDeleteTopic);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
        throw ex;
      }
    }
  }
}
