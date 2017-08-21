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
package com.streamsets.pipeline.stage.lib.kinesis;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.ArrayList;
import java.util.List;

public abstract class KinesisBaseUpgrader implements StageUpgrader {
  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  protected void moveConfigToBean(Config config, String beanName) {
    configsToRemove.add(config);
    configsToAdd.add(new Config(beanName + "." + config.getName(), config.getValue()));
  }

  protected void commitMove(List<Config> configs) {
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configsToRemove.clear();
    configsToAdd.clear();
  }

  protected void upgradeToCommonConfigBeanV1(List<Config> configs, String beanName) {
    for (Config config : configs) {
      // Migrate existing configs that were moved into the common Kinesis config bean
      switch (config.getName()) {
        case "region":
          // fall through
        case "streamName":
          // fall through:
        case "dataFormat":
          // fall through:
        case "awsAccessKeyId":
          // fall through
        case "awsSecretAccessKey":
          moveConfigToBean(config, beanName);
          break;
        default:
          // no-op
      }
    }

    commitMove(configs);
  }
}
