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

import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StringMapConfigPutUpgraderAction<T> extends UpgraderAction<StringListConfigAddUpgraderAction, T> {

  private String lookForName;
  private Object ifValueMatches = MATCHES_ALL;
  private String key;
  private Object value;

  public StringMapConfigPutUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public String getKey() {
    return key;
  }

  public StringMapConfigPutUpgraderAction setKey(String key) {
    this.key = key;
    return this;
  }

  public Object getValue() {
    return value;
  }

  public StringMapConfigPutUpgraderAction setValue(Object value) {
    this.value = value;
    return this;
  }

  public String getLookForName() {
    return lookForName;
  }

  public StringMapConfigPutUpgraderAction setLookForName(String lookForName) {
    this.lookForName = lookForName;
    return this;
  }

  public Object getIfValueMatches() {
    return ifValueMatches;
  }

  public StringMapConfigPutUpgraderAction setIfValueMatches(Object ifValueMatches) {
    if (ifValueMatches != null) {
      this.ifValueMatches = ifValueMatches;
    }
    return this;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    Utils.checkNotNull(getName(), "name");
    Utils.checkNotNull(getKey(), "key");
    Utils.checkNotNull(getValue(), "value");
    ConfigsAdapter configsAdapter = wrap(configs);
    ConfigsAdapter.Pair pair = configsAdapter.find(getName());
    boolean configFound = true;
    if(getLookForName()!=null) {
      configFound = existsConfigWithValue(getLookForName(), getIfValueMatches(), configsAdapter);
    }
    if (pair != null && configFound) {
      List<Map> entries = (List<Map>) pair.getValue();
      entries = (entries == null) ? new ArrayList<>() : new ArrayList<>(entries);
      int entryIdx = findInListMap(entries);
      Map entry = new HashMap();
      entry.put("key", key);
      entry.put("value", value);
      if (entryIdx == -1) {
        entries.add(entry);
      } else {
        entries.set(entryIdx, entry);
      }
      configsAdapter.set(getName(), entries);
    }
  }

  int findInListMap(List<Map> listMap) {
    for (int i = 0; i < listMap.size(); i++) {
      if (listMap.get(i).get("key").equals(key)) {
        return i;
      }
    }
    return -1;
  }

}
