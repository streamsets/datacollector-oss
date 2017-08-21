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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GeolocationProcessorUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
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
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("geoIP2DBType", "COUNTRY"));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("missingAddressAction", "REPLACE_WITH_NULLS"));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();

    String geoIP2DBFile = null;
    String geoIP2DBType = null;

    for (Config config : configs) {
      switch (config.getName()) {
        case "geoIP2DBFile":
          configsToRemove.add(config);
          geoIP2DBFile = config.getValue().toString();
          break;
        case "geoIP2DBType":
          configsToRemove.add(config);
          geoIP2DBType = config.getValue().toString();
          break;
      }
    }

    configs.removeAll(configsToRemove);

    List<Map<String, String>> dbConfigs = Lists.newArrayList();
    Map<String, String> dbConfig = ImmutableMap.of("geoIP2DBFile", geoIP2DBFile, "geoIP2DBType", geoIP2DBType);
    dbConfigs.add(dbConfig);
    configs.add(new Config("dbConfigs", dbConfigs));
  }
}
