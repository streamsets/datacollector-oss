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
package com.streamsets.pipeline.stage.origin.windows;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WindowsEventLogUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("customLogName", ""));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToRemove = configs.stream()
        .filter(config -> !config.getName().equals("stageOnRecordError"))
        .collect(Collectors.toList());
    List<Config> changedConfigs = configsToRemove.stream()
        .map(config -> new Config("commonConf." + config.getName(), config.getValue()))
        .collect(Collectors.toList());
    changedConfigs.add(new Config("commonConf.bufferSize", -1));
    changedConfigs.add(new Config("readerAPIType", "EVENT_LOGGING"));
    configs.removeAll(configsToRemove);
    configs.addAll(changedConfigs);
  }

}
