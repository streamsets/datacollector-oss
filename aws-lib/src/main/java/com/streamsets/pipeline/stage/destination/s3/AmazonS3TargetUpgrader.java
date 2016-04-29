/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;

import java.util.ArrayList;
import java.util.List;

public class AmazonS3TargetUpgrader implements StageUpgrader {
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
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
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
        case "s3TargetConfigBean.charset":
        case "s3TargetConfigBean.csvFileFormat":
        case "s3TargetConfigBean.csvHeader":
        case "s3TargetConfigBean.csvReplaceNewLines":
        case "s3TargetConfigBean.jsonMode":
        case "s3TargetConfigBean.textFieldPath":
        case "s3TargetConfigBean.textEmptyLineIfNull":
        case "s3TargetConfigBean.avroSchema":
        case "s3TargetConfigBean.binaryFieldPath":
          configsToRemove.add(config);
          configsToAdd.add(
              new Config(
                  "s3TargetConfigBean.dataGeneratorFormatConfig." + config.getName().substring(19),
                  config.getValue()
              )
          );
          break;
        default:
          // no upgrade needed
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configs.add(new Config("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter", '|'));
    configs.add(new Config("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomEscape", '\\'));
    configs.add(new Config("s3TargetConfigBean.dataGeneratorFormatConfig.csvCustomQuote", '\"'));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    AWSUtil.renameAWSCredentialsConfigs(configs);
    configs.add(new Config("s3TargetConfigBean.dataGeneratorFormatConfig.avroCompression", "NULL"));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "s3TargetConfigBean.s3Config.folder":
          configsToAdd.add(new Config("s3TargetConfigBean.s3Config.commonPrefix", config.getValue()));
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
