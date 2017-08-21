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
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;

public class FlumeTargetUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
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

  private static void upgradeV2ToV3(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
  }

  private void upgradeV1ToV2(List<Config> configs) {

    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for(Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
          configsToRemove.add(config);
          configsToAdd.add(new Config("flumeConfigBean." + config.getName(), config.getValue()));
          break;
        case "flumeHostsConfig":
        case "clientType":
        case "backOff":
        case "maxBackOff":
        case "hostSelectionStrategy":
        case "batchSize":
        case "connectionTimeout":
        case "requestTimeout":
        case "maxRetryAttempts":
        case "waitBetweenRetries":
        case "singleEventPerBatch":
          configsToRemove.add(config);
          configsToAdd.add(new Config("flumeConfigBean.flumeConfig." + config.getName(), config.getValue()));
          break;
        case "charset":
        case "csvFileFormat":
        case "csvHeader":
        case "csvReplaceNewLines":
        case "jsonMode":
        case "textFieldPath":
        case "textEmptyLineIfNull":
        case "avroSchema":
        case "includeSchema":
          configsToRemove.add(config);
          configsToAdd.add(new Config("flumeConfigBean.dataGeneratorFormatConfig." + config.getName(), config.getValue()));
          break;
        default:
          // no upgrade action required
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configs.add(new Config("flumeConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter", '|'));
    configs.add(new Config("flumeConfigBean.dataGeneratorFormatConfig.csvCustomEscape", '\\'));
    configs.add(new Config("flumeConfigBean.dataGeneratorFormatConfig.csvCustomQuote", '\"'));
    configs.add(new Config("flumeConfigBean.dataGeneratorFormatConfig.binaryFieldPath", "/"));
    configs.add(new Config("flumeConfigBean.dataGeneratorFormatConfig.avroCompression", "NULL"));
  }
}
