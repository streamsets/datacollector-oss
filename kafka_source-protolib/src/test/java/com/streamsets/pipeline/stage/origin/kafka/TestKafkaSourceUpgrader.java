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
package com.streamsets.pipeline.stage.origin.kafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestKafkaSourceUpgrader {

  @Test
  public void testUpgradeV3toV4() throws StageException {
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("dataFormat", DataFormat.TEXT));
    configs.add(new Config("metadataBrokerList", "MY_LIST"));
    configs.add(new Config("zookeeperConnect", "MY_ZK_CONNECTION"));
    configs.add(new Config("consumerGroup", "MY_GROUP"));
    configs.add(new Config("topic", "MY_TOPIC"));
    configs.add(new Config("produceSingleRecordPerMessage", false));
    configs.add(new Config("maxBatchSize", 1000));
    configs.add(new Config("maxWaitTime", 10));
    configs.add(new Config("kafkaConsumerConfigs", null));
    configs.add(new Config("charset", "UTF-8"));
    configs.add(new Config("removeCtrlChars", false));
    configs.add(new Config("textMaxLineLen", 1024));

    KafkaSourceUpgrader kafkaSourceUpgrader = new KafkaSourceUpgrader();
    kafkaSourceUpgrader.upgrade("a", "b", "c", 3, 4, configs);

    Assert.assertEquals(12, configs.size());

    HashMap<String, Object> configValues = new HashMap<>();
    for (Config c : configs) {
      configValues.put(c.getName(), c.getValue());
    }

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormat"));
    Assert.assertEquals(DataFormat.TEXT, configValues.get("kafkaConfigBean.dataFormat"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.metadataBrokerList"));
    Assert.assertEquals("MY_LIST", configValues.get("kafkaConfigBean.metadataBrokerList"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.zookeeperConnect"));
    Assert.assertEquals("MY_ZK_CONNECTION", configValues.get("kafkaConfigBean.zookeeperConnect"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.consumerGroup"));
    Assert.assertEquals("MY_GROUP", configValues.get("kafkaConfigBean.consumerGroup"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.topic"));
    Assert.assertEquals("MY_TOPIC", configValues.get("kafkaConfigBean.topic"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.produceSingleRecordPerMessage"));
    Assert.assertEquals(false, configValues.get("kafkaConfigBean.produceSingleRecordPerMessage"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.maxBatchSize"));
    Assert.assertEquals(1000, configValues.get("kafkaConfigBean.maxBatchSize"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.maxWaitTime"));
    Assert.assertEquals(10, configValues.get("kafkaConfigBean.maxWaitTime"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.kafkaConsumerConfigs"));
    Assert.assertEquals(null, configValues.get("kafkaConfigBean.kafkaConsumerConfigs"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormatConfig.charset"));
    Assert.assertEquals("UTF-8", configValues.get("kafkaConfigBean.dataFormatConfig.charset"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormatConfig.removeCtrlChars"));
    Assert.assertEquals(false, configValues.get("kafkaConfigBean.dataFormatConfig.removeCtrlChars"));

    Assert.assertTrue(configValues.containsKey("kafkaConfigBean.dataFormatConfig.textMaxLineLen"));
    Assert.assertEquals(1024, configValues.get("kafkaConfigBean.dataFormatConfig.textMaxLineLen"));
  }
}
