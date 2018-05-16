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

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class TestSparkStreamingBinding {

  @Test
  public void testClusterKafka_0_10_AutoOffsetReset() {
    Assert.assertEquals("largest", SparkStreamingBinding.getConfigurableAutoOffsetResetIfNonEmpty("latest"));
    Assert.assertEquals("smallest", SparkStreamingBinding.getConfigurableAutoOffsetResetIfNonEmpty("earliest"));
  }

  @Test
  public void testGetCheckPointPath() {
    Properties properties = new Properties();
    String topic = "topic";
    String consumerGroup = "consumerGroup";
    properties.put(ClusterModeConstants.CLUSTER_PIPELINE_NAME, "p:n");
    properties.put(SparkStreamingBinding.SDC_ID, "uuid1234");
    SparkStreamingBinding sparkStreamingBinding = new Kafka010SparkStreamingBinding(properties);
    CheckpointPath checkpointPath = sparkStreamingBinding.getCheckPointPath(topic, consumerGroup);
    StringBuilder expected = new StringBuilder();
    expected.append(SparkStreamingBinding.CHECKPOINT_BASE_DIR).
         append("/")
        .append("uuid1234")
        .append("/")
        .append(CheckpointPath.encode(topic))
        .append("/")
        .append(CheckpointPath.encode(consumerGroup))
        .append("/")
        .append(CheckpointPath.encode("p:n"));
    Assert.assertEquals(expected.toString(), checkpointPath.getPath());
  }

  @Test
  public void testGetConfigs() {
    Properties properties = new Properties();
    properties.put(Utils.KAFKA_CONFIG_BEAN_PREFIX + "topic", "topic");
    properties.put(Utils.KAFKA_CONFIG_BEAN_PREFIX + "consumerGroup", "consumerGroup");
    SparkStreamingBinding sparkStreamingBinding = new Kafka010SparkStreamingBinding(properties);
    Assert.assertEquals("topic", sparkStreamingBinding.getTopic());
    Assert.assertEquals("consumerGroup", sparkStreamingBinding.getConsumerGroup());
  }

  @Test
  public void testExtraConfigs() {
    Properties properties = new Properties();
    properties.put(ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX + "AAA", "ValueAAA");
    properties.put(ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX + "BBB", "ValueBBB");
    Map<String, String> extraMap = Utils.getExtraKafkaConfigs(properties);
    Assert.assertTrue(extraMap.containsKey("AAA"));
    Assert.assertEquals("ValueAAA", extraMap.get("AAA"));
    Assert.assertTrue(extraMap.containsKey("BBB"));
    Assert.assertEquals("ValueBBB", extraMap.get("BBB"));
  }
}
