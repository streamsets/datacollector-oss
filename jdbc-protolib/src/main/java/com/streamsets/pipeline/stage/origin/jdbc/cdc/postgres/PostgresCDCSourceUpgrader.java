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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import java.util.List;

public class PostgresCDCSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch (fromVersion) {
      case 1:
        configs.add(new Config("postgresCDCConfigBean.maxBatchWaitTime", 15000));
        if (toVersion == 2) {
          break;
        }
      case 2:
        configs.add(new Config("postgresCDCConfigBean.generatorQueueMaxSize", getMaxBatchSize(configs) * 5));
        configs.add(new Config("postgresCDCConfigBean.statusInterval", 30));
        if (toVersion == 3) {
          break;
        }

      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }

    return configs;
  }

  private int getMaxBatchSize(List<Config> configs) {
    for (Config config : configs) {
      if(config.getName().equals("postgresCDCConfigBean.baseConfigBean.maxBatchSize")) {
        return (int)config.getValue();
      }
    }

    return 1000;
  }

}
