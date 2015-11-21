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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.Compression;

import java.util.List;

public class SpoolDirSourceUpgrader implements StageUpgrader {

  public static final String FILE_COMPRESSION = "fileCompression";

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
                              List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
      case 2:
        upgradeV2ToV3(configs);
      case 3:
        upgradeV3ToV4(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
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
}
