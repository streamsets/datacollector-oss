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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RemoveConfigUpgraderAction<T> extends UpgraderAction<IterateConfigListUpgraderAction, T> {

  private String namePattern;

  public RemoveConfigUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public String getNamePattern() {
    return namePattern;
  }

  public RemoveConfigUpgraderAction setNamePattern(String namePattern) {
    this.namePattern = namePattern;
    return this;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    Utils.checkNotNull(getNamePattern(), "namePattern");
    Pattern pattern = Pattern.compile(getNamePattern());
    ConfigsAdapter configsAdapter = wrap(configs);

    Iterator<ConfigsAdapter.Pair> iterator = configsAdapter.iterator();
    while (iterator.hasNext()) {
      ConfigsAdapter.Pair config = iterator.next();
      Matcher matcher = pattern.matcher(config.getName());
      if (matcher.matches()) {
        configsAdapter.remove(config.getName());
      }
    }
  }

}
