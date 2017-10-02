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
package com.streamsets.pipeline.stage.bigquery.destination;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class BigQueryTargetUpgrader implements StageUpgrader {

  private static final String BIG_QUERY_TARGET_CONFIG_PREFIX = "conf.";
  private static final String IMPLICIT_FIELD_MAPPING_CONFIG =
      BIG_QUERY_TARGET_CONFIG_PREFIX  + "implicitFieldMapping";
  private static final String BIG_QUERY_IMPLICIT_FIELD_MAPPING_CONFIG =
      BIG_QUERY_TARGET_CONFIG_PREFIX + "bigQueryFieldMappingConfigs";
  private static final String MAX_CACHE_SIZE =
      BIG_QUERY_TARGET_CONFIG_PREFIX + "maxCacheSize";


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
        upgradeV1toV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  //Remove implicit field mapping
  private void upgradeV1toV2(List<Config> configs) {
    configs.removeIf(config -> (config.getName().equals(IMPLICIT_FIELD_MAPPING_CONFIG) ||
        config.getName().equals(BIG_QUERY_IMPLICIT_FIELD_MAPPING_CONFIG)));
    configs.add(new Config(MAX_CACHE_SIZE, -1));
  }
}
