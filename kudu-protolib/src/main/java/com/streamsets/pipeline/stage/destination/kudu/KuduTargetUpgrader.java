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
package com.streamsets.pipeline.stage.destination.kudu;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class KuduTargetUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "tableName":
          configsToAdd.add(new Config("tableNameTemplate", config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "kuduMaster":
          configsToRemove.add(config);
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "kuduMaster", config.getValue()));
          break;
        case "tableNameTemplate":
          configsToRemove.add(config);
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "tableNameTemplate", config.getValue()));
          break;
        case "fieldMappingConfigs":
          configsToRemove.add(config);
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "fieldMappingConfigs", config.getValue()));
          break;
        case "consistencyMode":
          configsToRemove.add(config);
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "consistencyMode", config.getValue()));
          break;
        case "operationTimeout":
          configsToRemove.add(config);
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "operationTimeout", config.getValue()));
          break;
        default:
          break;
      }
    }

    configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "upsert", false));

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private void upgradeV3ToV4(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();
    String upsert = KuduConfigBean.CONF_PREFIX + "upsert";

    for (Config config : configs) {
      if (config.getName().equals(upsert)){
        if ((Boolean)config.getValue()){
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "defaultOperation", "UPSERT"));
        } else {
          configsToAdd.add(new Config(KuduConfigBean.CONF_PREFIX + "defaultOperation", "INSERT"));
        }
        configsToRemove.add(config);
        break;
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config(KuduConfigBean.CONF_PREFIX + "adminOperationTimeout", 30000));
    configs.add(new Config(KuduConfigBean.CONF_PREFIX + "numWorkers", 0)); // use default
  }
}
