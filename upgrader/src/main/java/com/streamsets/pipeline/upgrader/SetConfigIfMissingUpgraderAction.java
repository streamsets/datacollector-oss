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

import com.streamsets.pipeline.api.impl.Utils;

import java.util.function.Function;

public class SetConfigIfMissingUpgraderAction<T> extends SetConfigUpgraderAction<T> {

  private String lookForName;

  public SetConfigIfMissingUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public String getLookForName() {
    return lookForName;
  }

  public SetConfigIfMissingUpgraderAction setLookForName(String lookForName) {
    this.lookForName = lookForName;
    return this;
  }

  @Override
  public void upgrade(T configs) {
    Utils.checkNotNull(getLookForName(), "lookForName");
    ConfigsAdapter configsAdapter = wrap(configs);
    if (configsAdapter.find(getLookForName()) == null) {
      super.upgrade(configs);
    }
  }

}
