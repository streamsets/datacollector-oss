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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;

import java.util.ArrayList;
import java.util.List;

public class HdfsTargetUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade (
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
        break;
      case 4:
      // handled by yaml upgrader
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
  }

  private static void upgradeV1ToV2(List<Config> configs) {

    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for(Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
          configsToRemove.add(config);
          configsToAdd.add(new Config("hdfsTargetConfigBean." + config.getName(), config.getValue()));
          break;
        case "hdfsUri":
        case "hdfsUser":
        case "hdfsKerberos":
        case "hdfsConfDir":
        case "hdfsConfigs":
        case "uniquePrefix":
        case "dirPathTemplate":
        case "timeZoneID":
        case "timeDriver":
        case "maxRecordsPerFile":
        case "maxFileSize":
        case "compression":
        case "otherCompression":
        case "fileType":
        case "keyEl":
        case "seqFileCompressionType":
        case "lateRecordsLimit":
        case "lateRecordsAction":
        case "lateRecordsDirPathTemplate":
          configsToRemove.add(config);
          configsToAdd.add(new Config("hdfsTargetConfigBean." + config.getName(), config.getValue()));
          break;
        case "charset":
        case "csvFileFormat":
        case "csvHeader":
        case "csvReplaceNewLines":
        case "jsonMode":
        case "textFieldPath":
        case "textEmptyLineIfNull":
        case "avroSchema":
        case "binaryFieldPath":
          configsToRemove.add(config);
          configsToAdd.add(new Config("hdfsTargetConfigBean.dataGeneratorFormatConfig." + config.getName(), config.getValue()));
          break;
        default:
          // no upgrade required
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configs.add(new Config("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter", '|'));
    configs.add(new Config("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape", '\\'));
    configs.add(new Config("hdfsTargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote", '\"'));
    configs.add(new Config("hdfsTargetConfigBean.dataGeneratorFormatConfig.avroCompression", "NULL"));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    // We've released version of HDFS Target that wasn't properly adding idleTimeout if it was missing. Hence
    // we can see both version 2 with and without this property, all depending upon on which particular SDC
    // version the pipeline was created.
    final String propertyName = "hdfsTargetConfigBean.idleTimeout";
    boolean found = false;
    for(Config config: configs) {
      if(propertyName.equals(config.getName())) {
        found = true;
        break;
      }
    }

    if(!found) {
      configs.add(new Config(propertyName, "-1"));
    }

    configs.add(new Config("hdfsTargetConfigBean.dirPathTemplateInHeader", false));
    configs.add(new Config("hdfsTargetConfigBean.rollIfHeader", false));
    configs.add(new Config("hdfsTargetConfigBean.rollHeaderName", "roll"));
  }
}
