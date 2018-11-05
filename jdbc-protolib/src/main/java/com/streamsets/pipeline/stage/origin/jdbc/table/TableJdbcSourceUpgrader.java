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
package com.streamsets.pipeline.stage.origin.jdbc.table;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.stage.origin.jdbc.CommonSourceConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;

public class TableJdbcSourceUpgrader implements StageUpgrader{
  private static final Logger LOG = LoggerFactory.getLogger(TableJdbcSourceUpgrader.class);

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
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(
        new Config(
            TableJdbcConfigBean.TABLE_JDBC_CONFIG_BEAN_PREFIX + TableJdbcConfigBean.BATCHES_FROM_THE_RESULT_SET,
            -1
        )
    );
    configs.add(
        new Config(
            TableJdbcConfigBean.TABLE_JDBC_CONFIG_BEAN_PREFIX + TableJdbcConfigBean.NUMBER_OF_THREADS,
            1
        )
    );
    configs.add(
        new Config(
            TableJdbcConfigBean.TABLE_JDBC_CONFIG_BEAN_PREFIX + TableJdbcConfigBean.QUOTE_CHAR,
            QuoteChar.NONE
        )
    );
    configs.add(
        new Config(
            CommonSourceConfigBean.COMMON_SOURCE_CONFIG_BEAN_PREFIX + CommonSourceConfigBean.NUM_SQL_ERROR_RETRIES,
            0
        )
    );
  }

  private void upgradeV2ToV3(List<Config> configs) {
    Config tableConfigs = UpgraderUtils.getConfigWithName(configs, TableJdbcConfigBean.TABLE_CONFIG);

    List<LinkedHashMap<String, Object>> tableConfigsMap =
        (List<LinkedHashMap<String, Object>>) tableConfigs.getValue();

    for (LinkedHashMap<String, Object> tableConfigMap : tableConfigsMap) {
      tableConfigMap.put(TableConfigBean.PARTITIONING_MODE_FIELD, PartitioningMode.DISABLED.name());
      tableConfigMap.put(TableConfigBean.PARTITION_SIZE_FIELD, TableConfigBean.DEFAULT_PARTITION_SIZE);
      tableConfigMap.put(
          TableConfigBean.MAX_NUM_ACTIVE_PARTITIONS_FIELD,
          TableConfigBean.DEFAULT_MAX_NUM_ACTIVE_PARTITIONS
      );
    }
  }

  private void upgradeV3ToV4(List<Config> configs) {
    Config tableConfigs = UpgraderUtils.getConfigWithName(configs, TableJdbcConfigBean.TABLE_CONFIG);

    List<LinkedHashMap<String, Object>> tableConfigsMap =
        (List<LinkedHashMap<String, Object>>) tableConfigs.getValue();

    for (LinkedHashMap<String, Object> tableConfigMap : tableConfigsMap) {
      tableConfigMap.put(
          TableConfigBean.ENABLE_NON_INCREMENTAL_FIELD,
          TableConfigBean.ENABLE_NON_INCREMENTAL_DEFAULT_VALUE
      );
    }
  }

  private void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config(TableConfigBean.ALLOW_LATE_TABLE, false));

    // upgrade queryInterval to queriesPerSecond
    final String numThreadsField = "tableJdbcConfigBean.numberOfThreads";
    final Config numThreadsConfig = UpgraderUtils.getConfigWithName(configs, numThreadsField);
    if (numThreadsConfig == null) {
      throw new IllegalStateException(String.format(
          "%s config was not found in configs: %s",
          numThreadsField,
          configs
      ));
    }

    int numThreads;
    final Object numThreadsObj = numThreadsConfig.getValue();
    if (numThreadsObj instanceof String) {
      // num threads is an EL expression, which we can't evaluate
      // the fact that the user entered an expression here (instead of going with the default value of "1") strongly
      // suggests there will be multiple, so go with 2
      numThreads = 2;
      LOG.info(
          "Could not evaluate expression {} for numThreads; for the purpose of upgrading config to queriesPerSecond," +
              " using a default value of {}",
          numThreadsObj,
          numThreads
      );
    } else if (numThreadsObj instanceof Integer) {
      numThreads = (int) numThreadsObj;
    } else {
      numThreads = 1;
      LOG.error(
          "Unrecognized type - {} - for numThreads value; for the purpose of upgrading config to queriesPerSecond," +
              " using a default value of {}",
          numThreadsObj,
          numThreads
      );
    }

    CommonSourceConfigBean.upgradeRateLimitConfigs(configs, "commonSourceConfigBean", numThreads);
  }
}
