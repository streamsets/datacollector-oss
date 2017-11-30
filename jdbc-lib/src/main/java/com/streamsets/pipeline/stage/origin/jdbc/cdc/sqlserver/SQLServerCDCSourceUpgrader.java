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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver;

import com.google.common.base.Strings;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLServerCDCSourceUpgrader implements StageUpgrader {
  public static final String TABLECONFIG = "cdcTableJdbcConfigBean.tableConfigs";
  public static final String ALLOW_LATE_TABLE = "commonSourceConfigBean.allowLateTable";
  private static final String SCHEMA_CONFIG = "schema";
  private static final String TABLEPATTERN_CONFIG = "tablePattern";
  private static final String TABLE_EXCLUSION_CONFIG = "tableExclusionPattern";
  private static final String TABLE_INITIALOFFSET_CONFIG = "initialOffset";
  private static final String TABLE_CAPTURE_INSTANCE_CONFIG = "capture_instance";
  private static final String TABLE_TIMEZONE_ID = "cdcTableJdbcConfigBean.timeZoneID";
  private static final String SQL_SERVER_CDC_TABLE_NAME_SUFFIX = "_CT";

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
    Config removeConfig = null;
    Config addConfig = null;

    configs.add(new Config(ALLOW_LATE_TABLE, false));

    for (Config config : configs) {
      if (TABLECONFIG.equals(config.getName())) {
        List<Map<String, String>> tableConfig = (List<Map<String, String>>) config.getValue();
        ArrayList<Map<String, String>> newconfig = new ArrayList<>();

        for (Map<String, String> tableConfig2 : tableConfig) {
          String tablePattern = tableConfig2.get(TABLEPATTERN_CONFIG);
          String schema = tableConfig2.get(SCHEMA_CONFIG);
          String initialOffset = tableConfig2.get(TABLE_INITIALOFFSET_CONFIG);
          String tableExclusion = tableConfig2.get(TABLE_EXCLUSION_CONFIG);

          StringBuilder captureInstance = new StringBuilder();
          captureInstance.append(Strings.isNullOrEmpty(schema) ? "%" : schema);
          captureInstance.append("_");
          captureInstance.append(Strings.isNullOrEmpty(tablePattern) ? "%" : tablePattern);

          Map<String, String> newTableConfig = new HashMap<>();
          newTableConfig.put(TABLE_CAPTURE_INSTANCE_CONFIG, captureInstance.toString());

          if (!Strings.isNullOrEmpty(initialOffset)) {
            newTableConfig.put(TABLE_INITIALOFFSET_CONFIG, initialOffset);
          }

          if (!Strings.isNullOrEmpty(tableExclusion)) {
            newTableConfig.put(TABLE_EXCLUSION_CONFIG, tableExclusion);
          }

          newconfig.add(newTableConfig);
        }
        removeConfig = config;
        addConfig = new Config(TABLECONFIG, newconfig);

        break;
      }
    }

    if (removeConfig != null) {
      configs.add(addConfig);
      configs.remove(removeConfig);
    }
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    for (Config config : configs) {
      if (TABLE_TIMEZONE_ID.equals(config.getName())) {
        configs.remove(config);
        break;
      }
    }

    // upgrade queryInterval to queriesPerSecond
    final String numThreadsField = "cdcTableJdbcConfigBean.numberOfThreads";
    final Config numThreadsConfig = UpgraderUtils.getConfigWithName(configs, numThreadsField);
    if (numThreadsConfig == null) {
      throw new IllegalStateException(String.format(
          "%s config was not found in configs: %s",
          numThreadsField,
          configs
      ));
    }
    final int numThreads = (int) numThreadsConfig.getValue();

    CommonSourceConfigBean.upgradeRateLimitConfigs(configs, "commonSourceConfigBean", numThreads);
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    // SDC-7990. in v3, the work around was adding "_CT" at the end of capture_instance.
    // This is not anymore true, so removing "_CT" suffix is handled by upgrader
    Config removeConfig = null;
    Config addConfig = null;

    for (Config config : configs) {
      if (TABLECONFIG.equals(config.getName())) {
        List<Map<String, String>> tableConfig = (List<Map<String, String>>) config.getValue();
        ArrayList<Map<String, String>> newconfig = new ArrayList<>();

        for (Map<String, String> tableConfig2 : tableConfig) {
          String captureInstanceName = tableConfig2.get(TABLE_CAPTURE_INSTANCE_CONFIG);
          String initialOffset = tableConfig2.get(TABLE_INITIALOFFSET_CONFIG);
          String tableExclusion = tableConfig2.get(TABLE_EXCLUSION_CONFIG);

          Map<String, String> newTableConfig = new HashMap<>();
          if (captureInstanceName.endsWith(SQL_SERVER_CDC_TABLE_NAME_SUFFIX)) {
            int lastIndex = captureInstanceName.lastIndexOf(SQL_SERVER_CDC_TABLE_NAME_SUFFIX);
            String newCaptureInstanceName = captureInstanceName.substring(0, lastIndex);
            newTableConfig.put(TABLE_CAPTURE_INSTANCE_CONFIG, newCaptureInstanceName);
          } else {
            newTableConfig.put(TABLE_CAPTURE_INSTANCE_CONFIG, captureInstanceName);
          }

          if (!Strings.isNullOrEmpty(initialOffset)) {
            newTableConfig.put(TABLE_INITIALOFFSET_CONFIG, initialOffset);
          }

          if (!Strings.isNullOrEmpty(tableExclusion)) {
            newTableConfig.put(TABLE_EXCLUSION_CONFIG, tableExclusion);
          }

          newconfig.add(newTableConfig);
        }
        removeConfig = config;
        addConfig = new Config(TABLECONFIG, newconfig);

        break;
      }
    }

    if (removeConfig != null) {
      configs.add(addConfig);
      configs.remove(removeConfig);
    }
  }
}
