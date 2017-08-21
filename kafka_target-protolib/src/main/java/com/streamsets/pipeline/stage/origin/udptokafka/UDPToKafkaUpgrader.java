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
package com.streamsets.pipeline.stage.origin.udptokafka;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;


public class UDPToKafkaUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("udpConfigs.enableEpoll", false));
  }

  // Version 3 is renaming configs that should have been renamed in version 2, but sadly we've made the release
  private static void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      if (config.getName().startsWith("kafkaConfigBean.kafkaConfig")) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(
            config.getName().replace("kafkaConfigBean.kafkaConfig", "kafkaTargetConfig"),
            config.getValue()
        ));
      } else if (config.getName().startsWith("kafkaConfigBean.dataGeneratorFormatConfig")) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(
          config.getName().replace("kafkaConfigBean.dataGeneratorFormatConfig", "kafkaTargetConfig.dataGeneratorFormatConfig"),
          config.getValue()
        ));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

}
