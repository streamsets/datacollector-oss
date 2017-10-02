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
package com.streamsets.pipeline.stage.origin.opcua;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class OpcUaClientSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        //fall through
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.nodeIdFetchMode":
        case "conf.nodeIdConfigsFilePath":
          configsToRemove.add(config);
      }
    }
    configs.removeAll(configsToRemove);
    configs.add(new Config("conf.nodeIdFetchMode", "MANUAL"));
    configs.add(new Config(
        "conf.nodeIdConfigsFilePath",
        "${runtime:loadResource(\'nodeIdConfigs.json\', false)}"
    ));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    for (Config config : configs) {
      switch (config.getName()) {
        case "conf.rootIdentifier":
        case "conf.rootIdentifierType":
        case "conf.rootNamespaceIndex":
        case "conf.refreshNodeIdsInterval":
        case "conf.sessionTimeoutMillis":
          configsToRemove.add(config);
      }
    }
    configs.removeAll(configsToRemove);
    configs.add(new Config("conf.rootIdentifier", ""));
    configs.add(new Config("conf.rootIdentifierType", "NUMERIC"));
    configs.add(new Config("conf.rootNamespaceIndex", 0));
    configs.add(new Config("conf.refreshNodeIdsInterval", 3600));
    configs.add(new Config("conf.sessionTimeoutMillis", 120000));

  }
}
