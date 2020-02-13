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
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IterateConfigListUpgraderAction extends UpgraderAction<IterateConfigListUpgraderAction, List<Config>> {

  private List<UpgraderAction<?, Map<String, Object>>> actions = Collections.emptyList();

  public IterateConfigListUpgraderAction() {
    super(CONFIGS_WRAPPER);
  }

  public List<UpgraderAction<?, Map<String, Object>>> getActions() {
    return actions;
  }

  public IterateConfigListUpgraderAction setActions(List<UpgraderAction<?, Map<String, Object>>> actions) {
    this.actions = actions;
    return this;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      List<Config> configs
  ) {
    Utils.checkNotNull(getName(), "name");
    ConfigsAdapter wrapper = wrap(configs);
    ConfigsAdapter.Pair configsAdapter = wrapper.find(getName());
    if (configsAdapter == null) {
      throw new StageException(Errors.YAML_UPGRADER_03, getName());
    }
    List<Map<String, Object>> listConfig = (List<Map<String, Object>>) configsAdapter.getValue();
    listConfig = (listConfig == null) ? Collections.emptyList() : listConfig;

    //ensuring the List element Map configs are modifiable maps.
    listConfig = listConfig.stream().map(HashMap::new).collect(Collectors.toList());
    for (Map<String, Object> element : listConfig) {
      for (UpgraderAction<?, Map<String, Object>> action : getActions()) {
        action.upgrade(context, originalConfigs, element);
      }
    }

    // as we streamed the original list value to convert Maps to modifiable, we need to set the new list in the config
    wrapper.set(getName(), listConfig);
  }

}
