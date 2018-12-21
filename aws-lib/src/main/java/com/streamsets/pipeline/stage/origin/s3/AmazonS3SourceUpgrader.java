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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AmazonS3SourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        // fall through
      case 6:
        upgradeV6ToV7(configs);
        // fall through
      case 7:
        upgradeV7ToV8(configs);
        // fall through
      case 8:
        upgradeV8ToV9(configs);
        // fall through
      case 9:
        upgradeV9ToV10(configs, context);
        // fall through
      case 10:
        upgradeV10ToV11(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.useProxy", false));
    configs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyHost", ""));
    configs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyPort", 0));
    configs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyUser", ""));
    configs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyPassword", ""));
    configs.add(new Config(S3ConfigBean.S3_DATA_FORMAT_CONFIG_PREFIX + "compression", "NONE"));
    configs.add(new Config(S3ConfigBean.S3_DATA_FORMAT_CONFIG_PREFIX + "filePatternInArchive", "*"));
    configs.add(new Config(S3ConfigBean.S3_DATA_FORMAT_CONFIG_PREFIX + "csvSkipStartLines", 0));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    AWSUtil.renameAWSCredentialsConfigs(configs);
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case S3ConfigBean.S3_CONFIG_PREFIX + "folder":
          configsToAdd.add(new Config(S3ConfigBean.S3_CONFIG_PREFIX + "commonPrefix", config.getValue()));
          configsToRemove.add(config);
          break;
        case S3ConfigBean.S3_FILE_CONFIG_PREFIX + "filePattern":
          configsToAdd.add(new Config(S3ConfigBean.S3_FILE_CONFIG_PREFIX + "prefixPattern", config.getValue()));
          configsToRemove.add(config);
          break;
        case S3ConfigBean.ERROR_CONFIG_PREFIX + "errorFolder":
          configsToAdd.add(new Config(S3ConfigBean.ERROR_CONFIG_PREFIX + "errorPrefix", config.getValue()));
          configsToRemove.add(config);
          break;
        case S3ConfigBean.ERROR_CONFIG_PREFIX + "archivingOption":
          if ("MOVE_TO_DIRECTORY".equals(config.getValue())) {
            configsToAdd.add(new Config(S3ConfigBean.ERROR_CONFIG_PREFIX + "archivingOption", "MOVE_TO_PREFIX"));
            configsToRemove.add(config);
          }
          break;
        case S3ConfigBean.POST_PROCESSING_CONFIG_PREFIX + "postProcessFolder":
          configsToAdd.add(new Config(S3ConfigBean.POST_PROCESSING_CONFIG_PREFIX + "postProcessPrefix", config.getValue()));
          configsToRemove.add(config);
          break;
        case S3ConfigBean.POST_PROCESSING_CONFIG_PREFIX + "archivingOption":
          if ("MOVE_TO_DIRECTORY".equals(config.getValue())) {
            configsToAdd.add(new Config(S3ConfigBean.POST_PROCESSING_CONFIG_PREFIX + "archivingOption", "MOVE_TO_PREFIX"));
            configsToRemove.add(config);
          }
          break;
        default:
          // no op
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    // rename advancedConfig to proxyConfig
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.useProxy":
        case S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyHost":
        case S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyPort":
        case S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyUser":
        case S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "advancedConfig.proxyPassword":
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
  }

  private static void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(S3ConfigBean.S3_FILE_CONFIG_PREFIX + "objectOrdering", ObjectOrdering.TIMESTAMP));
  }

  private static void upgradeV6ToV7(List<Config> configs) {
    configs.add(new Config(S3ConfigBean.S3_CONFIG_PREFIX + "endpoint", ""));
  }

  private static void upgradeV7ToV8(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private static void upgradeV8ToV9(List<Config> configs) {
    configs.add(new Config(S3ConfigBean.S3_SSE_CONFIG_PREFIX + "useCustomerSSEKey", false));
  }

  private static void upgradeV9ToV10(List<Config> configs, Context context) {
    List<Config> dataFormatConfigs = configs.stream()
      .filter(c -> c.getName().startsWith("s3ConfigBean.dataFormat"))
      .collect(Collectors.toList());

    // Remove those configs
    configs.removeAll(dataFormatConfigs);

    // Add this config conditionally - some older versions might be missing it
    if(dataFormatConfigs.stream().noneMatch(c -> c.getName().contains("useCustomDelimiter"))) {
      dataFormatConfigs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "dataFormatConfig.useCustomDelimiter", false));
    }

    // Remove SQS specific prefix
    dataFormatConfigs = dataFormatConfigs.stream()
      .map(c -> new Config(c.getName().replace("s3ConfigBean.", ""), c.getValue()))
      .collect(Collectors.toList());

    // And finally register new service
    context.registerService(DataFormatParserService.class, dataFormatConfigs);
  }

  private static void upgradeV10ToV11(List<Config> configs) {
    configs.add(new Config(S3ConfigBean.S3_CONFIG_BEAN_PREFIX + "numberOfThreads", 1));
  }
}
