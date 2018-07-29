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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisBaseUpgrader;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;
import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.LEASE_TABLE_BEAN;

public class KinesisSourceUpgrader extends KinesisBaseUpgrader {

  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1toV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2toV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3toV4(configs);
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4toV5(configs);
        if (toVersion == 5) {
          break;
        }
        // fall through
      case 5:
        configs = upgradeV5toV6(configs);
        if (toVersion == 6) {
          break;
        }
        // fall through
      case 6:
        return upgradeV6toV7(configs);
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private List<Config> upgradeV6toV7(List<Config> configs) {
    configs.add(new Config(Joiner.on(".").join(KINESIS_CONFIG_BEAN, LEASE_TABLE_BEAN, "tags"), Collections.emptyMap()));
    return configs;
  }

  private void upgradeV1toV2(List<Config> configs) {
    upgradeToCommonConfigBeanV1(configs, KINESIS_CONFIG_BEAN);
    upgradeToConsumerConfigBeanV1(configs);
  }

  private void upgradeToConsumerConfigBeanV1(List<Config> configs) {
    for (Config config : configs) {
      // Migrate existing configs that were moved into the Kinesis Consumer config bean
      switch (config.getName()) {
        case "applicationName":
          // fall through
        case "maxBatchSize":
          // fall through
        case "idleTimeBetweenReads":
          // fall through
        case "maxWaitTime":
          // fall through
        case "previewWaitTime":
          moveConfigToBean(config, KINESIS_CONFIG_BEAN);
          break;
        default:
          // no-op
      }
    }
    commitMove(configs);

    configs.add(new Config(KINESIS_CONFIG_BEAN + ".initialPositionInStream", InitialPositionInStream.LATEST));
  }

  private static void upgradeV2toV3(List<Config> configs) {
    AWSUtil.renameAWSCredentialsConfigs(configs);

    configs.add(new Config(KINESIS_CONFIG_BEAN + ".dataFormatConfig.csvSkipStartLines", 0));
  }

  private static void upgradeV3toV4(List<Config> configs) {
    configs.add(new Config(KINESIS_CONFIG_BEAN + ".endpoint", ""));
  }

  private static void upgradeV4toV5(List<Config> configs) {
    DataFormatUpgradeHelper.ensureAvroSchemaExists(configs, KINESIS_CONFIG_BEAN + ".dataFormatConfig");
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private List<Config> upgradeV5toV6(List<Config> configs) {
    configs.add(new Config(KINESIS_CONFIG_BEAN + ".maxRecordProcessors", "${runtime:availableProcessors()}"));
    return configs.stream()
        .filter(c -> !c.getName().endsWith("WaitTime")) // removes previewWaitTime and maxWaitTime
        .collect(Collectors.toList());
  }
}
