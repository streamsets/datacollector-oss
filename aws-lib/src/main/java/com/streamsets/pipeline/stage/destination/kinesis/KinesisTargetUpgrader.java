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
package com.streamsets.pipeline.stage.destination.kinesis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisBaseUpgrader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.stage.lib.kinesis.KinesisUtil.KINESIS_CONFIG_BEAN;

public class KinesisTargetUpgrader extends KinesisBaseUpgrader {

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
        // fall through
      case 2:
        upgradeV2toV3(configs);
        // fall through
      case 3:
        upgradeV3toV4(configs);
        // fall through
      case 4:
        upgradeV4toV5(configs);
        // fall through
      case 5:
        upgradeV5toV6(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV5toV6(List<Config> configs) {
    DataFormatUpgradeHelper.ensureAvroSchemaExists(configs, KINESIS_CONFIG_BEAN + ".dataFormatConfig");
    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
  }

  private void upgradeV1toV2(List<Config> configs) {
    upgradeToCommonConfigBeanV1(configs, KINESIS_CONFIG_BEAN);
    upgradeToProducerConfigBeanV1(configs);
  }

  private void upgradeToProducerConfigBeanV1(List<Config> configs) {
    for (Config config : configs) {
      // Migrate existing configs that were moved into the Kinesis Producer config bean
      switch (config.getName()) {
        case "partitionStrategy":
          // fall through
          moveConfigToBean(config, KINESIS_CONFIG_BEAN);
          break;
        default:
          // no-op
      }
    }
    commitMove(configs);

    configs.add(new Config(KINESIS_CONFIG_BEAN + ".partitionExpression", "${0}"));
    configs.add(new Config(KINESIS_CONFIG_BEAN + ".preserveOrdering", false));
    configs.add(new Config(KINESIS_CONFIG_BEAN + ".producerConfigs", new ArrayList<Map<String, String>>()));
  }

  private static void upgradeV2toV3(List<Config> configs) {
    AWSUtil.renameAWSCredentialsConfigs(configs);

    configs.add(new Config(KINESIS_CONFIG_BEAN + ".dataFormatConfig.avroCompression", "NULL"));
  }

  private static void upgradeV3toV4(List<Config> configs) {
    Config roundRobinPartitioner = null;

    for (Config config : configs) {
      if ((KINESIS_CONFIG_BEAN + ".partitionStrategy").equals(config.getName()) &&
          PartitionStrategy.ROUND_ROBIN.toString().equals(config.getValue())
          ) {
        roundRobinPartitioner = config;
      }
    }

    if (roundRobinPartitioner != null) {
      configs.remove(roundRobinPartitioner);
      configs.add(new Config(KINESIS_CONFIG_BEAN + ".partitionStrategy", PartitionStrategy.RANDOM));
    }
  }

  private static void upgradeV4toV5(List<Config> configs) {
    configs.add(new Config(KINESIS_CONFIG_BEAN + ".endpoint", ""));
  }
}
