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
package com.streamsets.pipeline.stage.processor.hive;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class HiveMetadataProcessorUpgrader implements StageUpgrader {
  private static final String DATA_FORMAT = "dataFormat";
  private static final String DATA_FORMAT_AVRO = HMPDataFormat.AVRO.name();
  private static final String COMMENT_EXPRESSION = "commentExpression";
  private static final String CONVERT_TIMES_TO_STRING = "convertTimesToString";

  @Override
  public List<Config> upgrade(
      String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToAdd = new ArrayList<>();

    configsToAdd.add(new Config(DATA_FORMAT, DATA_FORMAT_AVRO));
    configsToAdd.add(new Config(COMMENT_EXPRESSION, ""));

    configs.addAll(configsToAdd);
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config(CONVERT_TIMES_TO_STRING, true));
  }
}
