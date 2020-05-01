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

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

public class SetConfigUpgraderAction<T> extends UpgraderAction<SetConfigUpgraderAction, T> {

  public static final String MATCHES_ALL = ".*";

  private String lookForName;
  private Object ifValueMatches = MATCHES_ALL;
  private Object value;
  private String elseName;
  private Object elseValue;

  public SetConfigUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public Object getValue() {
    return value;
  }

  public SetConfigUpgraderAction setValue(Object value) {
    this.value = value;
    return this;
  }

  public String getLookForName() {
    return lookForName;
  }

  public SetConfigUpgraderAction setLookForName(String lookForName) {
    this.lookForName = lookForName;
    return this;
  }

  public Object getIfValueMatches() {
    return ifValueMatches;
  }

  public SetConfigUpgraderAction setIfValueMatches(Object ifValueMatches) {
    if (ifValueMatches != null) {
      this.ifValueMatches = ifValueMatches;
    }
    return this;
  }

  public String getElseName() {
    return elseName;
  }

  public SetConfigUpgraderAction setElseName(String elseName) {
    this.elseName = elseName;
    return this;
  }

  public Object getElseValue() {
    return elseValue;
  }

  public SetConfigUpgraderAction setElseValue(Object elseValue) {
    this.elseValue = elseValue;
    return this;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    if (getName() == null && getElseName() == null) {
      throw new NullPointerException("either name or elseName must be not null");
    } else if (getName() != null && getValue() == null) {
      throw new NullPointerException("value cannot be null when name is set");
    } else if (getElseName() != null && getElseValue() == null) {
      throw new NullPointerException("elseValue cannot be null when elseName is set");
    }

    ConfigsAdapter configsAdapter = wrap(configs);

    if (getLookForName() == null) {
      configsAdapter.set(getName(), resolveValueIfEL(configsAdapter.toConfigMap(), getValue()));
    } else {
      boolean configFound = existsConfigWithValue(getLookForName(), getIfValueMatches(), configsAdapter);
      if (getName() != null && configFound) {
        configsAdapter.set(getName(), resolveValueIfEL(configsAdapter.toConfigMap(), getValue()));
      } else if (getElseName() != null && !configFound) {
        configsAdapter.set(getElseName(), resolveValueIfEL(configsAdapter.toConfigMap(), getElseValue()));
      }
    }
  }

  /**
   * Check if a configuration exists and has a given value.  If the configuration value is null, it'll match against
   * empty string.
   *
   * @param name    Name of the configuration to look for.
   * @param value   Expected value for the configuration. Use {@value MATCHES_ALL} to accept any value.
   * @param adapter Adapter used to find the configuration.
   * @return true if {@code name} exists and equals {@code value}, false otherwise.
   */
  private boolean existsConfigWithValue(String name, Object value, ConfigsAdapter adapter) {
    ConfigsAdapter.Pair config = adapter.find(name);
    if (config != null) {
      if (MATCHES_ALL.equals(value)) {
        return true;
      } else {
        boolean matches;
        Object configValue = config.getValue() != null ? config.getValue() : "";
        if ((configValue instanceof String)) {
          Pattern pattern = Pattern.compile(value.toString());
          matches = pattern.matcher(configValue.toString()).matches();
        } else {
          matches = configValue.equals(value);
        }
        return matches;
      }
    }
    return false;
  }
}
