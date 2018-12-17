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
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AmazonS3TargetUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    int fromVersion = context.getFromVersion();
    int toVersion = context.getToVersion();

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
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        if (toVersion == 5) {
          break;
        }
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        if (toVersion == 6) {
          break;
        }
        // fall through
      case 6:
        upgradeV6ToV7(configs);
        if (toVersion == 7) {
          break;
        }
        // fall through
      case 7:
        upgradeV7ToV8(configs);
        if (toVersion == 8) {
          break;
        }
        // fall through
      case 8:
        upgradeV8ToV9(configs);
        if (toVersion == 9) {
          break;
        }
        // fall through
      case 9:
        upgradeV9ToV10(configs);
        if(toVersion == 10) {
          break;
        }
        // fall through
      case 10:
        upgradeV10toV11(configs, context);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV8ToV9(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
  }

  private static void upgradeV1ToV2(List<Config> configs) {

    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "charset":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "csvFileFormat":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "csvHeader":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "csvReplaceNewLines":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "jsonMode":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "textFieldPath":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "textEmptyLineIfNull":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "avroSchema":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "binaryFieldPath":
          configsToRemove.add(config);
          configsToAdd.add(
              new Config(
                  S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig." + config.getName().substring(19),
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

    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.csvCustomDelimiter", '|'));
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.csvCustomEscape", '\\'));
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.csvCustomQuote", '\"'));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    AWSUtil.renameAWSCredentialsConfigs(configs);
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.avroCompression", "NULL"));
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case S3TargetConfigBean.S3_CONFIG_PREFIX + "folder":
          configsToAdd.add(new Config(S3TargetConfigBean.S3_CONFIG_PREFIX + "commonPrefix", config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);

    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "partitionTemplate", ""));
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config(S3TargetConfigBean.S3_SSE_CONFIG_PREFIX + "useSSE", false));
  }

  private static void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "timeZoneID", "UTC"));
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "timeDriverTemplate", "${time:now()}"));
  }

  private static void upgradeV6ToV7(List<Config> configs) {
    // rename advancedConfig to proxyConfig
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "advancedConfig.useProxy":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "advancedConfig.proxyHost":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "advancedConfig.proxyPort":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "advancedConfig.proxyUser":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "advancedConfig.proxyPassword":
          configsToRemove.add(config);
          configsToAdd.add(new Config(config.getName().replace("advancedConfig", "proxyConfig"), config.getValue()));
          break;
        default:
          // no upgrade needed
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    // add transfer manager configs
    configs.add(new Config(S3TargetConfigBean.S3_TM_CONFIG_PREFIX + "threadPoolSize", 10));
    configs.add(new Config(S3TargetConfigBean.S3_TM_CONFIG_PREFIX + "minimumUploadPartSize", 5242880));
    configs.add(new Config(S3TargetConfigBean.S3_TM_CONFIG_PREFIX + "multipartUploadThreshold", 268435456));
  }

  private static void upgradeV7ToV8(List<Config> configs) {
    configs.add(new Config(S3TargetConfigBean.S3_CONFIG_PREFIX + "endpoint", ""));
  }

  private static void upgradeV9ToV10(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for(Config config : configs) {
      if(config.getName().equals(S3TargetConfigBean.S3_CONFIG_PREFIX + "bucket")) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(S3TargetConfigBean.S3_CONFIG_PREFIX + "bucketTemplate", config.getValue()));
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV10toV11(List<Config> configs, Context context) {
    List<Config> dataFormatConfigs = configs.stream()
      .filter(c -> c.getName().startsWith("s3TargetConfigBean.dataGeneratorFormat") || c.getName().startsWith("s3TargetConfigBean.dataFormat"))
      .collect(Collectors.toList());

    // Remove those configs
    configs.removeAll(dataFormatConfigs);

    // Depending on when exactly this stage was created, this config might be missing and in that case we need to add it
    // before migrating to services.
    final String textFieldMissingAction = "s3TargetConfigBean.dataGeneratorFormatConfig.textFieldMissingAction";
    if(dataFormatConfigs.stream().noneMatch(c -> textFieldMissingAction.equals(c.getName()))) {
      dataFormatConfigs.add(new Config(textFieldMissingAction, "ERROR"));
    }

    // Provide proper prefix
    dataFormatConfigs = dataFormatConfigs.stream()
      .map(c -> new Config(c.getName().replace("s3TargetConfigBean.", ""), c.getValue()))
      .collect(Collectors.toList());

    // And finally register new service
    context.registerService(DataFormatGeneratorService.class, dataFormatConfigs);
  }

}
