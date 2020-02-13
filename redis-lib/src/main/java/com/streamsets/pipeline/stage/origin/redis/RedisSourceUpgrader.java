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
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;

public class RedisSourceUpgrader implements StageUpgrader {
  private static final String DATA_FORMAT_CONFIG = "redisOriginConfigBean.advancedConfig.";

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(
      String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1toV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2toV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1toV2(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "redisOriginConfigBean.readStrategy":
        case "redisOriginConfigBean.queueName":
        case DATA_FORMAT_CONFIG + "keysPattern":
        case DATA_FORMAT_CONFIG + "namespaceSeparator":
        case DATA_FORMAT_CONFIG + "executionTimeout":
          configsToRemove.add(config);
          break;
        case DATA_FORMAT_CONFIG + "connectionTimeout":
          configsToAdd.add(new Config("redisOriginConfigBean.connectionTimeout", config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          //no-op
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV2toV3(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }
}
