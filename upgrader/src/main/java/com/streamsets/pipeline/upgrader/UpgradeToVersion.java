/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.upgrader;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpgradeToVersion {
  List<UpgraderAction<?, List<Config>>> actions = Collections.emptyList();

  public List<UpgraderAction<?, List<Config>>> getActions() {
    return actions;
  }

  public UpgradeToVersion setActions(List<UpgraderAction<?, List<Config>>> actions) {
    this.actions = actions;
    return this;
  }

  public void upgrade(List<Config> configs, StageUpgrader.Context context) {
    // we are not doing Java8 streams magic because value can be NULL and streams list to map fails with NPE
    Map<String, Object> originalConfigs = new HashMap<>();
    for (Config config : configs) {
      originalConfigs.put(config.getName(), config.getValue());
    }
    for (UpgraderAction<?, List<Config>> action : actions) {
      action.upgrade(context, originalConfigs, configs);
    }
  }

}
