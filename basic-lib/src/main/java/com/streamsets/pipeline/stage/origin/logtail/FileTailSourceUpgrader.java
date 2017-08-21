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
package com.streamsets.pipeline.stage.origin.logtail;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;

public class FileTailSourceUpgrader implements StageUpgrader {

  private static final String CONF = "conf";
  private static final String DATA_FORMAT_CONFIG= "dataFormatConfig";
  private static final String ALLOW_LATE_DIRECTORIES = "allowLateDirectories";
  private static final Joiner joiner = Joiner.on(".");

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
                              List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        //fall through
      case 2:
        upgradeV2ToV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    DataFormatUpgradeHelper.ensureAvroSchemaExists(configs, joiner.join(CONF, DATA_FORMAT_CONFIG));
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private void upgradeV1ToV2(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "multiLineMainPattern":
        case "batchSize":
        case "maxWaitTimeSecs":
        case "fileInfos":
        case "postProcessing":
        case "archiveDir":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "charset":
        case "removeCtrlChars":
        case "logMode":
        case "logMaxObjectLen":
        case "retainOriginalLine":
        case "customLogFormat":
        case "regex":
        case "fieldPathsToGroupName":
        case "grokPatternDefinition":
        case "grokPattern":
        case "enableLog4jCustomLogFormat":
        case "log4jCustomLogFormat":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "maxLineLength":
          // Data format can be Text, JSON, or LOG. So we set max length for all of them.
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "textMaxLineLen"), config.getValue()));
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "jsonMaxObjectLen"), config.getValue()));
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "logMaxObjectLen"), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          break;
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, ALLOW_LATE_DIRECTORIES), false));
  }

}
