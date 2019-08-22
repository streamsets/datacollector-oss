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
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.lib.kafka.KafkaAutoOffsetReset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaSourceUpgrader implements StageUpgrader {

  private static final String CONF = "kafkaConfigBean";
  private static final String DATA_FORMAT_CONFIG= "dataFormatConfig";
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
        // handled by YAML upgrader
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("csvCustomDelimiter", '|'));
    configs.add(new Config("csvCustomEscape", '\\'));
    configs.add(new Config("csvCustomQuote", '\"'));
    configs.add(new Config("csvRecordType", "LIST"));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("csvSkipStartLines", 0));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "metadataBrokerList":
        case "zookeeperConnect":
        case "consumerGroup":
        case "topic":
        case "produceSingleRecordPerMessage":
        case "maxBatchSize":
        case "maxWaitTime":
        case "kafkaConsumerConfigs":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "charset":
        case "removeCtrlChars":
        case "textMaxLineLen":
        case "jsonContent":
        case "jsonMaxObjectLen":
        case "csvFileFormat":
        case "csvHeader":
        case "csvMaxObjectLen":
        case "csvCustomDelimiter":
        case "csvCustomEscape":
        case "csvCustomQuote":
        case "csvRecordType":
        case "csvSkipStartLines":
        case "xmlRecordElement":
        case "xmlMaxObjectLen":
        case "logMode":
        case "logMaxObjectLen":
        case "retainOriginalLine":
        case "customLogFormat":
        case "regex":
        case "fieldPathsToGroupName":
        case "grokPatternDefinition":
        case "grokPattern":
        case "onParseError":
        case "maxStackTraceLines":
        case "enableLog4jCustomLogFormat":
        case "log4jCustomLogFormat":
        case "schemaInMessage":
        case "avroSchema":
        case "binaryMaxObjectLen":
        case "protoDescriptorFile":
        case "messageType":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no-op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "keyDeserializer"), Deserializer.STRING));
    configs.add(new Config(joiner.join(CONF, "valueDeserializer"), Deserializer.DEFAULT));

    DataFormatUpgradeHelper.ensureAvroSchemaExists(configs, joiner.join(CONF, DATA_FORMAT_CONFIG));
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }


  private static void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "timestampsEnabled"), false));
  }

  private static void upgradeV6ToV7(List<Config> configs) {
    String autoOffsetReset = "null";
    for (Config config : configs) {
      if ("kafkaOptions".equals(config.getName())) {
        Map<String, String> kafkaOptions = (Map<String, String>) config.getValue();
        autoOffsetReset = kafkaOptions.remove("auto.offset.reset");
      }
    }

    switch (autoOffsetReset) {
      case "earliest":
        configs.add(new Config(joiner.join(CONF, "kafkaAutoOffsetReset"), KafkaAutoOffsetReset.EARLIEST));
        break;
      case "latest":
        configs.add(new Config(joiner.join(CONF, "kafkaAutoOffsetReset"), KafkaAutoOffsetReset.LATEST));
        break;
      case "none":
        configs.add(new Config(joiner.join(CONF, "kafkaAutoOffsetReset"), KafkaAutoOffsetReset.NONE));
        break;
      default:
        configs.add(new Config(joiner.join(CONF, "kafkaAutoOffsetReset"), KafkaAutoOffsetReset.EARLIEST));
    }

    configs.add(new Config(joiner.join(CONF, "timestampToSearchOffsets"), 0));
  }
}
