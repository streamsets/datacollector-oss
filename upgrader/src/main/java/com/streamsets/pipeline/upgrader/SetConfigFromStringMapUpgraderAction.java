/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SetConfigFromStringMapUpgraderAction<T> extends UpgraderAction<SetConfigFromStringMapUpgraderAction, T> {

  private String mapName;
  private String key;

  public SetConfigFromStringMapUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public String getMapName() {
    return mapName;
  }

  public SetConfigFromStringMapUpgraderAction setMapName(String mapName) {
    this.mapName = mapName;
    return this;
  }

  public String getKey() {
    return key;
  }

  public SetConfigFromStringMapUpgraderAction setKey(String mapKey) {
    this.key = mapKey;
    return this;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    Utils.checkNotNull(getName(), "name");
    Utils.checkNotNull(getMapName(), "mapName");
    Utils.checkNotNull(getKey(), "mapKey");

    ConfigsAdapter configsAdapter = wrap(configs);
    ConfigsAdapter.Pair configMapPair = configsAdapter.find(getMapName());

    if (configMapPair != null) {
      List<Map> entries = (List<Map>) configMapPair.getValue();
      entries = entries == null ? new ArrayList<>() : new ArrayList<>(entries);
      for (Map entry : entries) {
        if (entry.get("key").equals(getKey())) {
          configsAdapter.set(getName(), entry.get("value"));
        }
      }
    }
  }
}
