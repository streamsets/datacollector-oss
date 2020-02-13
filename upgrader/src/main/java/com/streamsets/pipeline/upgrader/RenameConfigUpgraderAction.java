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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RenameConfigUpgraderAction<T> extends UpgraderAction<RenameConfigUpgraderAction, T> {

  private String oldNamePattern;
  private String newNamePattern;

  public RenameConfigUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public String getOldNamePattern() {
    return oldNamePattern;
  }

  public RenameConfigUpgraderAction setOldNamePattern(String oldNamePattern) {
    this.oldNamePattern = oldNamePattern;
    return this;
  }

  public String getNewNamePattern() {
    return newNamePattern;
  }

  public RenameConfigUpgraderAction setNewNamePattern(String newNamePattern) {
    this.newNamePattern = newNamePattern;
    return this;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    Utils.checkNotNull(getOldNamePattern(), "oldNamePattern");
    Utils.checkNotNull(getNewNamePattern(), "newNamePattern");
    Pattern pattern = Pattern.compile(getOldNamePattern());
    ConfigsAdapter configsAdapter = wrap(configs);
    for (ConfigsAdapter.Pair pair : configsAdapter) {
      Matcher matcher = pattern.matcher(pair.getName());
      if (matcher.matches()) {
        String newName = getNewNamePattern();
        for (int gIdx = 0; gIdx < matcher.groupCount(); gIdx++) {
          if (matcher.group(gIdx) != null) {
            newName = newName.replace("(" + gIdx + ")", matcher.group(gIdx + 1));
          } else {
            throw new StageException(Errors.YAML_UPGRADER_02, pair.getName(), getOldNamePattern(), gIdx);
          }
        }
        configsAdapter.remove(pair.getName());
        configsAdapter.set(newName, pair.getValue());
      }
    }
  }

}
