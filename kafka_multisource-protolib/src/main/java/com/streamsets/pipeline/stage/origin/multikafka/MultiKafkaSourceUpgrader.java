/**
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
package com.streamsets.pipeline.stage.origin.multikafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.List;
import java.util.Map;

public class MultiKafkaSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(
      String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
      case 3:
        // handled by YAML upgrader
        if (toVersion == 3) {
          break;
        }
        // handled by YAML upgrader
        if (toVersion == 4) {
          break;
        }
        // fall through
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    String autoOffsetReset = "null";
    for (Config config : configs) {
      if ("kafkaOptions".equals(config.getName())) {
        Map<String, String> kafkaOptions = (Map<String, String>) config.getValue();
        autoOffsetReset = kafkaOptions.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
      }
    }

    switch (autoOffsetReset) {
      case "earliest":
        configs.add(new Config("conf.kafkaAutoOffsetReset", KafkaAutoOffsetReset.EARLIEST));
        break;
      case "latest":
        configs.add(new Config("conf.kafkaAutoOffsetReset", KafkaAutoOffsetReset.LATEST));
        break;
      case "none":
        configs.add(new Config("conf.kafkaAutoOffsetReset", KafkaAutoOffsetReset.NONE));
        break;
      default:
        configs.add(new Config("conf.kafkaAutoOffsetReset", KafkaAutoOffsetReset.EARLIEST));
    }

    configs.add(new Config("conf.timestampToSearchOffsets", 0));
  }
}
