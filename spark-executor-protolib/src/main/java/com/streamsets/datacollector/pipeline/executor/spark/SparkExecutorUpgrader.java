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
package com.streamsets.datacollector.pipeline.executor.spark;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.stream.Collectors;

public class SparkExecutorUpgrader implements StageUpgrader {

  @Override
  public List<Config> upgrade(List<Config> configs, StageUpgrader.Context context) throws StageException {
    int fromVersion = context.getFromVersion();
    switch (fromVersion) {
      case 1:
        return upgradeV1ToV2(configs);
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));

    }
  }

  private static List<Config> upgradeV1ToV2(List<Config> configs) {
    return configs.parallelStream()
        .filter(config -> !config.getName().startsWith("conf.databricks")
            && !config.getName().equals("conf.credentialsConfigBean.username")
            && !config.getName().equals("conf.credentialsConfigBean.password")
            && !config.getName().equals("conf.clusterManager")
        )
        .collect(Collectors.toList());
  }
}
