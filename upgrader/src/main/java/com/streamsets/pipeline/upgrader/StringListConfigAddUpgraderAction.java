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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StringListConfigAddUpgraderAction<T> extends UpgraderAction<StringListConfigAddUpgraderAction, T> {

  private String lookForName;
  private Object ifValueMatches = MATCHES_ALL;
  private String value;

  public StringListConfigAddUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public String getValue() {
    return value;
  }

  public StringListConfigAddUpgraderAction setValue(String value) {
    this.value = value;
    return this;
  }

  public String getLookForName() {
    return lookForName;
  }

  public StringListConfigAddUpgraderAction setLookForName(String lookForName) {
    this.lookForName = lookForName;
    return this;
  }

  public Object getIfValueMatches() {
    return ifValueMatches;
  }

  public StringListConfigAddUpgraderAction setIfValueMatches(Object ifValueMatches) {
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
    Utils.checkNotNull(getValue(), "value");
    ConfigsAdapter configsAdapter = wrap(configs);
    ConfigsAdapter.Pair pair = configsAdapter.find(getName());

    boolean configFound = true;
    if (getLookForName()!=null) {
      configFound = existsConfigWithValue(getLookForName(), getIfValueMatches(), configsAdapter);
    }
    if (configFound) {
      List<String> entries = (pair == null) ? new ArrayList<>() : (List<String>) pair.getValue();
      entries = (entries == null) ? new ArrayList<>() : new ArrayList<>(entries);
      entries.add(resolveValueIfEL(originalConfigs, value).toString());
      configsAdapter.set(getName(), entries);
    }
  }

}
