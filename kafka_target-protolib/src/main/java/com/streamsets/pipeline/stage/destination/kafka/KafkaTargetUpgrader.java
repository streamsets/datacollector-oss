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
package com.streamsets.pipeline.stage.destination.kafka;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.kafka.api.ProducerKeyFormat;
import com.streamsets.pipeline.stage.destination.lib.ResponseType;

import java.util.ArrayList;
import java.util.List;

public class KafkaTargetUpgrader implements StageUpgrader {
  private static final Joiner joiner = Joiner.on(".");

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
        // fall-through to next version
      case 2:
        upgradeV2ToV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall-through to next version
      case 3:
        upgradeV3ToV4(configs);
        if (toVersion == 4) {
          break;
        }
        // fall-through to next version
      case 4:
        // handled by yaml upgrader
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {

    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for(Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kafkaConfigBean." + config.getName(), config.getValue()));
          break;
        case "metadataBrokerList":
        case "runtimeTopicResolution":
        case "topicExpression":
        case "topicWhiteList":
        case "topic":
        case "partitionStrategy":
        case "partition":
        case "singleMessagePerBatch":
        case "kafkaProducerConfigs":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kafkaConfigBean.kafkaConfig." + config.getName(), config.getValue()));
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
        case "binaryFieldPath":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kafkaConfigBean.dataGeneratorFormatConfig." + config.getName(), config.getValue()));
          break;
        default:
          // no upgrade required
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configs.add(new Config("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomDelimiter", '|'));
    configs.add(new Config("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomEscape", '\\'));
    configs.add(new Config("kafkaConfigBean.dataGeneratorFormatConfig.csvCustomQuote", '\"'));
    configs.add(new Config("kafkaConfigBean.dataGeneratorFormatConfig.avroCompression", "NULL"));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    // Step 1: Rename kafkaConfigBean to conf
    // Step 2: Move kafkaConfigBean.kafkaConfig.* to conf.
    for (Config config : configs) {
      if (config.getName().startsWith("kafkaConfigBean")) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(
            config.getName().replace("kafkaConfigBean", "conf").replace("kafkaConfig.", ""),
            config.getValue()
        ));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configs.add(new Config(joiner.join("conf", "keySerializer"), Serializer.STRING));
    configs.add(new Config(joiner.join("conf", "valueSerializer"), Serializer.DEFAULT));

    DataFormatUpgradeHelper.upgradeAvroGeneratorWithSchemaRegistrySupport(configs);
  }

  private void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("responseConf.sendResponseToOrigin", false));
    configs.add(new Config("responseConf.responseType", ResponseType.SUCCESS_RECORDS));
  }
}
