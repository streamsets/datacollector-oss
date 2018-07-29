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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;

public class ClusterHdfsSourceUpgrader implements StageUpgrader {

  private static final String CONF = "clusterHDFSConfigBean";
  private static final String DATA_FORMAT_CONFIG = "dataFormatConfig";
  private static final Joiner joiner = Joiner.on(".");

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

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
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF,"awsAccessKey"), ""));
    configs.add(new Config(joiner.join(CONF,"awsSecretKey"), ""));
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("csvRecordType", "LIST"));
    configs.add(new Config("avroSchema", null));
    configs.add(new Config("csvFileFormat", "CSV"));
    configs.add(new Config("csvHeader", "NO_HEADER"));
    configs.add(new Config("csvMaxObjectLen", 1024));
    configs.add(new Config("csvCustomDelimiter", "|"));
    configs.add(new Config("csvCustomEscape", "\\"));
    configs.add(new Config("csvCustomQuote", "\""));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("csvSkipStartLines", 0));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "hdfsUri":
        case "hdfsDirLocations":
        case "hdfsKerberos":
        case "hdfsConfDir":
        case "hdfsUser":
        case "hdfsConfigs":
        case "recursive":
        case "produceSingleRecordPerMessage":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "avroSchema":
        case "textMaxLineLen":
        case "jsonMaxObjectLen":
        case "logMaxObjectLen":
        case "logMode":
        case "retainOriginalLine":
        case "customLogFormat":
        case "regex":
        case "fieldPathsToGroupName":
        case "grokPatternDefinition":
        case "grokPattern":
        case "enableLog4jCustomLogFormat":
        case "log4jCustomLogFormat":
        case "csvFileFormat":
        case "csvHeader":
        case "csvMaxObjectLen":
        case "csvCustomDelimiter":
        case "csvCustomEscape":
        case "csvCustomQuote":
        case "csvRecordType":
        case "csvSkipStartLines":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);

  }

}
