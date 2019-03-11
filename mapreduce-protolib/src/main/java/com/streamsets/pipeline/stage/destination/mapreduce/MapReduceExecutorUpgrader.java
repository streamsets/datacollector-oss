/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.mapreduce;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;

import java.util.List;

public class MapReduceExecutorUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(
      List<Config> configs, Context context
  ) throws StageException {

    final int fromVersion = context.getFromVersion();
    final int toVersion = context.getToVersion();

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
        throw new IllegalStateException(String.format("Unexpected fromVersion %d", fromVersion));
    }

    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    UpgraderUtils.moveAllTo(
        configs,
        "jobConfig.avroParquetConfig.inputFile",
        "jobConfig.avroConversionCommonConfig.inputFile",
        "jobConfig.avroParquetConfig.outputDirectory",
        "jobConfig.avroConversionCommonConfig.outputDirectory",
        "jobConfig.avroParquetConfig.keepInputFile",
        "jobConfig.avroConversionCommonConfig.keepInputFile",
        "jobConfig.avroParquetConfig.overwriteTmpFile",
        "jobConfig.avroConversionCommonConfig.overwriteTmpFile"
    );
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("jobConfig.avroParquetConfig.timeZoneID", "UTC"));
  }
}
