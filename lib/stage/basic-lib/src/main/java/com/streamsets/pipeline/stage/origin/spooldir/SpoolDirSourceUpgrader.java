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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.upgrade.DataFormatUpgradeHelper;
import com.streamsets.pipeline.lib.dirspooler.FileOrdering;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;

import java.util.ArrayList;
import java.util.List;

public class SpoolDirSourceUpgrader implements StageUpgrader {

  private static final String CONF = "conf";
  private static final String DATA_FORMAT_CONFIG= "dataFormatConfig";
  private static final String FILE_COMPRESSION = "fileCompression";
  private static final String ALLOW_LATE_DIRECTORY = "allowLateDirectory";
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
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV9ToV10(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "spoolingPeriod"), 5));
  }

  private void upgradeV8ToV9(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "pathMatcherMode"), PathMatcherMode.GLOB));
  }

  private void upgradeV7ToV8(List<Config> configs) {
    DataFormatUpgradeHelper.upgradeAvroParserWithSchemaRegistrySupport(configs);
  }

  private void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, ALLOW_LATE_DIRECTORY), false));
  }

  private void upgradeV4ToV5(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "spoolDir":
        case "overrunLimit":
        case "batchSize":
        case "poolingTimeoutSecs":
        case "filePattern":
        case "maxSpoolFiles":
        case "initialFileToProcess":
        case "errorArchiveDir":
        case "postProcessing":
        case "archiveDir":
        case "retentionTimeMins":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "charset":
        case "removeCtrlChars":
        case "filePatternInArchive":
        case "csvFileFormat":
        case "csvHeader":
        case "csvMaxObjectLen":
        case "csvCustomDelimiter":
        case "csvCustomEscape":
        case "csvCustomQuote":
        case "csvRecordType":
        case "jsonContent":
        case "jsonMaxObjectLen":
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
        case "avroSchema":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "fileCompression":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "compression"), config.getValue()));
          configsToRemove.add(config);
          break;
        case "textMaxObjectLen":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "textMaxLineLen"), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }

    configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "csvSkipStartLines"), 0));

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);

  }

  private void upgradeV3ToV4(List<Config> configs) {
    // change compression enum values
    Config compressionConfig = null;
    int compressionConfigIndex = -1;
    for(int i = 0; i < configs.size(); i++) {
      if(FILE_COMPRESSION.equals(configs.get(i).getName())) {
        switch((String)configs.get(i).getValue()) {
          case "AUTOMATIC" :
          case "NONE":
            compressionConfig = new Config(FILE_COMPRESSION, Compression.NONE.name());
            break;
          case "ZIP":
            compressionConfig = new Config(FILE_COMPRESSION, Compression.ARCHIVE.name());
            break;
          case "GZIP":
            compressionConfig = new Config(FILE_COMPRESSION, Compression.COMPRESSED_FILE.name());
            break;
          default:
            // no action needed
            break;
        }
        compressionConfigIndex = i;
        break;
      }
    }
    if(compressionConfigIndex != -1) {
      configs.remove(compressionConfigIndex);
      configs.add(compressionConfigIndex, compressionConfig);
    }

    // add compression file pattern string
    configs.add(new Config("filePatternInArchive", "*"));
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("fileCompression", "AUTOMATIC"));
    configs.add(new Config("csvCustomDelimiter", '|'));
    configs.add(new Config("csvCustomEscape", '\\'));
    configs.add(new Config("csvCustomQuote", '\"'));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("csvRecordType", "LIST"));
  }

  private void upgradeV6ToV7(List<Config> configs) {
    configs.add(new Config(joiner.join(CONF, "useLastModified"), FileOrdering.LEXICOGRAPHICAL.name()));
  }
}
