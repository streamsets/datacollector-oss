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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class OracleCDCSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance,
                              int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch (fromVersion) {
      case 1:
        configs = upgradeV1ToV2(configs);
        if (toVersion == 2) {
          return configs;
        }
        // fall through
      case 2:
        configs = upgradeV2ToV3(configs);
        if (toVersion == 3) {
          return configs;
        }
        // fall through
      case 3:
        configs = upgradeV3ToV4(configs);
        if (toVersion == 4) {
          return configs;
        }
        // fall through
      case 4:
        configs = upgradeV4ToV5(configs);
        if (toVersion == 5) {
          return configs;
        }
        // fall through
      case 5:
        return upgradeV5ToV6(configs);

      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
  }

  private static List<Config> upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("oracleCDCConfigBean.txnWindow", "${1 * HOURS}"));
    configs.add(new Config("oracleCDCConfigBean.logminerWindow", "${2 * HOURS}"));
    return configs;
  }

  private static List<Config> upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("oracleCDCConfigBean.bufferLocally", false));
    configs.add(new Config("oracleCDCConfigBean.discardExpired", false));
    configs.add(new Config("oracleCDCConfigBean.unsupportedFieldOp", UnsupportedFieldTypeValues.TO_ERROR));
    configs.add(new Config("oracleCDCConfigBean.keepOriginalQuery", false));
    configs.add(new Config("oracleCDCConfigBean.dbTimeZone", ZoneId.systemDefault().getId()));
    configs.add(new Config("oracleCDCConfigBean.queryTimeout", "${5 * MINUTES}"));
    return configs;
  }

  private static List<Config> upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("oracleCDCConfigBean.jdbcFetchSize", 1));
    return configs;
  }

  private static List<Config> upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config("oracleCDCConfigBean.sendUnsupportedFields", false));
    return configs;
  }

  @SuppressWarnings("unchecked")
  private static List<Config> upgradeV5ToV6(List<Config> configs) {
    List<Config> configsToSave = configs.stream().filter(config ->
        config.getName().equals("oracleCDCConfigBean.baseConfigBean.database") ||
        config.getName().equals("oracleCDCConfigBean.baseConfigBean.tables") ||
        config.getName().equals("oracleCDCConfigBean.baseConfigBean.excludePattern")
    ).collect(Collectors.toList());

    configs.removeAll(configsToSave);

    String schema = null;
    List<String> tables = null;
    String excludePattern = null;

    for (Config config : configsToSave) {
      switch (config.getName()) {
        case "oracleCDCConfigBean.baseConfigBean.database":
          schema = (String) config.getValue();
          break;
        case "oracleCDCConfigBean.baseConfigBean.tables":
          tables = (List<String>) config.getValue();
          break;
        case "oracleCDCConfigBean.baseConfigBean.excludePattern":
          excludePattern = (String) config.getValue();
          break;
      }
    }

    List<LinkedHashMap<String, Object>> schemaTables = new ArrayList<>();

    LinkedHashMap<String, Object> schemaTable = new LinkedHashMap<>();
    schemaTable.put("schema", schema);
    schemaTable.put("tables", tables);
    schemaTable.put("excludePattern", excludePattern);

    schemaTables.add(schemaTable);

    configs.add(new Config("oracleCDCConfigBean.baseConfigBean.schemaTableConfigs", schemaTables));

    return configs;
  }
}
