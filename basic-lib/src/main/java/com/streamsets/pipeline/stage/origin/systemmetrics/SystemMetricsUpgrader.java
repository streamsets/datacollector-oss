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
package com.streamsets.pipeline.stage.origin.systemmetrics;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class SystemMetricsUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    switch(context.getFromVersion()) {
      case 1:
        upgradeV1ToV2(configs, context);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", context.getFromVersion()));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs, Context context) {
    configs.add(new Config("conf.fetchProcessStats", false));
    configs.add(new Config("conf.processConf.processRegexStr", ".*"));
    configs.add(new Config("conf.processConf.userRegexStr", ".*"));
  }
}
