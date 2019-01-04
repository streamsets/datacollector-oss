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
package com.streamsets.pipeline.stage.devtest.rawdata;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RawDataSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs);
        if (context.getToVersion() == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs, context);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  // Avro support removed as it does not make sense for a text area.
  private static void upgradeV1ToV2(List<Config> configs) {
    List<Config> toRemove = new ArrayList<>();

    for (Config config : configs) {
      if (config.getName().contains("schemaInMessage")) {
        toRemove.add(config);
      } else if (config.getName().contains("avro")) {
        toRemove.add(config);
      }
    }

    // Add this config conditionally - some older versions will have it whereas some will not
    if(configs.stream().noneMatch(c -> c.getName().equals("dataFormatConfig.compression"))) {
      configs.add(new Config("dataFormatConfig.compression", "NONE"));
    }
    configs.removeAll(toRemove);
  }

  private static void upgradeV2ToV3(List<Config> configs, Context context) {
    List<Config> dataFormatConfigs = configs.stream()
      .filter(c -> c.getName().startsWith("dataFormat"))
      .collect(Collectors.toList());

    // Remove those configs
    configs.removeAll(dataFormatConfigs);

    // And finally register new service
    context.registerService(DataFormatParserService.class, dataFormatConfigs);
  }
}
