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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HiveQueryExecutorUpgrader implements StageUpgrader {
  static final String HIVE_QUERY_EXECUTOR_CONFIG_PREFIX = "config.";
  static final String STOP_ON_QUERY_FAILURE = "stopOnQueryFailure";
  static final String QUERY = "query";
  static final String QUERIES = "queries";

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToAdd = new ArrayList<>();
    List<Config> configsToRemove = new ArrayList<>();
    configsToAdd.add(new Config(HIVE_QUERY_EXECUTOR_CONFIG_PREFIX + STOP_ON_QUERY_FAILURE, true));

    for (Config config : configs) {
      if (config.getName().equals(HIVE_QUERY_EXECUTOR_CONFIG_PREFIX + QUERY)) {
        configsToRemove.add(config);
        String query = (String) config.getValue();
        configsToAdd.add(new Config(HIVE_QUERY_EXECUTOR_CONFIG_PREFIX + QUERIES, Collections.singletonList(query)));
      }
    }
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
