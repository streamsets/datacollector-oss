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
package com.streamsets.pipeline.stage.processor.hbase;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.Map;

public class HBaseProcessorUpgrader implements StageUpgrader {
  private static final String ZOOKEEPER_PARENT_ZNODE_OLD = "conf.hBaseConnectionConfig.zookeeperParentZnode";
  private static final String ZOOKEEPER_PARENT_ZNODE = "conf.hBaseConnectionConfig.zookeeperParentZNode";

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
        upgradeV2toV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  @SuppressWarnings("unchecked")
  private static void upgradeV1ToV2(List<Config> configs) {

    final String lookupsConfigName = "conf.lookups";
    final String tablesConfigName = "tables";

    for (Config config : configs) {
      if (lookupsConfigName.equals(config.getName())) {
        List<Map<String, String>> lookupConfigs = (List<Map<String, String>>) config.getValue();
        for (Map<String, String> lookupConfig : lookupConfigs) {
          lookupConfig.remove(tablesConfigName);
        }
      }
    }
  }

  private static void upgradeV2toV3(List<Config> configs) {
    Config oldZnodeConfig = null;
    for (Config config : configs) {
      if (ZOOKEEPER_PARENT_ZNODE_OLD.equals(config.getName())) {
        oldZnodeConfig = config;
        break;
      }
    }

    if (oldZnodeConfig != null) {
      configs.add(new Config(ZOOKEEPER_PARENT_ZNODE, oldZnodeConfig.getValue()));
      configs.remove(oldZnodeConfig);
    }
  }
}
