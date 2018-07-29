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
package com.streamsets.pipeline.stage.origin.udp;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;

import java.util.List;

public class UDPSourceUpgrader implements StageUpgrader {
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
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("enableEpoll", false));
    configs.add(new Config("numThreads", 1));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("rawDataMode", UDPSourceConfigBean.DEFAULT_RAW_DATA_MODE));
    configs.add(new Config("rawDataCharset", UDPSourceConfigBean.DEFAULT_RAW_DATA_CHARSET));
    configs.add(new Config("rawDataOutputField", UDPSourceConfigBean.DEFAULT_RAW_DATA_OUTPUT_FIELD));
    configs.add(new Config(
        "rawDataMultipleValuesBehavior",
        UDPSourceConfigBean.DEFAULT_RAW_DATA_MULTI_VALUES_BEHAVIOR
    ));
    configs.add(new Config("rawDataSeparatorBytes", UDPSourceConfigBean.DEFAULT_RAW_DATA_SEPARATOR_BYTES));
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    UpgraderUtils.prependToAll(
        configs,
        UDPDSource.CONFIG_PREFIX,
        //NOTE: ideally, would use StageConfigBean constants here, but those are defined in container :(
        "stageOnRecordError",
        "stageRequiredFields",
        "stageRecordPreconditions"
    );
  }
}
