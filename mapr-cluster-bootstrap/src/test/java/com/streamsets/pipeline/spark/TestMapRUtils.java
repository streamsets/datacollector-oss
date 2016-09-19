/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class TestMapRUtils {

  @Test
  public void testGetConfigs() {
    Properties properties = new Properties();
    properties.put(Utils.MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "topic", "topic");
    properties.put(Utils.MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxWaitTime", "4000");
    properties.put(AbstractStreamingBinding.AUTO_OFFSET_RESET, "smallest");
    properties.put(Utils.MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "consumerGroup", "consumerGroup");
    properties.put(Utils.MAPR_STREAMS_SOURCE_CONFIG_BEAN_PREFIX + "maxBatchSize", "2000");
    MapRStreamingBinding mapRStreamingBinding = new MapRStreamingBinding(properties);
    Assert.assertEquals("topic",mapRStreamingBinding.getTopic());
    Assert.assertEquals(4000, Utils.getMaprStreamsWaitTime(properties));
    Assert.assertEquals("smallest", Utils.getPropertyOrEmptyString(properties, AbstractStreamingBinding
        .AUTO_OFFSET_RESET));
    Assert.assertEquals("consumerGroup", Utils.getMaprStreamsConsumerGroup(properties));
    Assert.assertEquals(2000, Utils.getMaprStreamsMaxBatchSize(properties));
  }
}
