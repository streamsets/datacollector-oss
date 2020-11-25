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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SetConfigFromJoinedConfigListAction<T> extends UpgraderAction<SetConfigFromJoinedConfigListAction, T> {

  private String listName;
  private String delimiter;

  public SetConfigFromJoinedConfigListAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context, Map<String, Object> originalConfigs, T configs
  ) {
    Utils.checkNotNull(getName(), "name");
    Utils.checkNotNull(getListName(), "listName");
    Utils.checkNotNull(getListName(), "delimiter");

    ConfigsAdapter configsAdapter = wrap(configs);
    ConfigsAdapter.Pair configListPair = configsAdapter.find(getListName());

    if(configListPair == null) {
      throw new StageException(Errors.YAML_UPGRADER_03, getListName());
    }

    List<String> configValues = (List<String>) configListPair.getValue();

    configsAdapter.set(getName(), String.join(delimiter, configValues));
  }


  public String getDelimiter() {
    return delimiter;
  }

  public SetConfigFromJoinedConfigListAction<T> setDelimiter(String delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  public String getListName() {
    return listName;
  }

  public SetConfigFromJoinedConfigListAction<T> setListName(String listName) {
    this.listName = listName;
    return this;
  }
}
