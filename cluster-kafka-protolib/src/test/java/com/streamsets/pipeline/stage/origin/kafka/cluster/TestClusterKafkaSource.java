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

import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.pipeline.stage.origin.kafka.KafkaConfigBean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static com.streamsets.pipeline.Utils.KAFKA_CONFIG_BEAN_PREFIX;

public class TestClusterKafkaSource {

  @Test
  public void testGetConfigsToShip() throws Exception {
    KafkaConfigBean kafkaConfigBean = new KafkaConfigBean();
    kafkaConfigBean.kafkaConsumerConfigs.put("AAA", "ValueAAA");
    kafkaConfigBean.kafkaConsumerConfigs.put("BBB", "ValueBBB");
    kafkaConfigBean.metadataBrokerList = "localhost:9092";
    kafkaConfigBean.zookeeperConnect = "localhost:2181";
    kafkaConfigBean.consumerGroup = "group1";
    kafkaConfigBean.topic = "sdctopic";
    ClusterKafkaSource clusterKafkaSource = Mockito.spy(new ClusterKafkaSource(kafkaConfigBean));
    Mockito.doReturn(3).when(clusterKafkaSource).getParallelism();
    Assert.assertEquals(7, clusterKafkaSource.getConfigsToShip().size());
    Map<String, String> gotMap = clusterKafkaSource.getConfigsToShip();
    Assert.assertEquals("ValueAAA", gotMap.get(ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX + "AAA"));
    Assert.assertEquals("ValueBBB", gotMap.get(ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX + "BBB"));
    Assert.assertEquals("localhost:9092", gotMap.get(KAFKA_CONFIG_BEAN_PREFIX + "metadataBrokerList"));
    Assert.assertEquals("localhost:2181", gotMap.get(KAFKA_CONFIG_BEAN_PREFIX + "zookeeperConnect"));
    Assert.assertEquals("group1", gotMap.get(KAFKA_CONFIG_BEAN_PREFIX + "consumerGroup"));
    Assert.assertEquals("sdctopic", gotMap.get(KAFKA_CONFIG_BEAN_PREFIX + "topic"));
  }
}
