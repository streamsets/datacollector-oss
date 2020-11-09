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

public class ReplaceConfigUpgraderAction<T> extends UpgraderAction<ReplaceConfigUpgraderAction, T> {

  public static final String MATCHES_ALL = ".*";

  public static final String DEFAULT_TOKEN = "$$";

  private Object ifOldValueMatches = MATCHES_ALL;
  private String tokenForOldValue = DEFAULT_TOKEN;
  private Object newValue;
  private Integer newValueFromMatchIndex;
  private Object elseNewValue;

  public ReplaceConfigUpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public Object getIfOldValueMatches() {
    return ifOldValueMatches;
  }

  public ReplaceConfigUpgraderAction setIfOldValueMatches(Object ifOldValueMatches) {
    if (ifOldValueMatches != null) {
      this.ifOldValueMatches = ifOldValueMatches;
    }
    return this;
  }

  public String getTokenForOldValue() {
    return tokenForOldValue;
  }

  public ReplaceConfigUpgraderAction setTokenForOldValue(String tokenForOldValue) {
    if (tokenForOldValue != null) {
      this.tokenForOldValue = tokenForOldValue;
    }
    return this;
  }

  public Object getNewValue() {
    return newValue;
  }

  public ReplaceConfigUpgraderAction setNewValue(Object newValue) {
    this.newValue = newValue;
    return this;
  }

  public ReplaceConfigUpgraderAction setNewValueFromMatchIndex(Object matchIndex) {
    if (matchIndex instanceof Integer) {
      this.newValueFromMatchIndex = ((Integer) matchIndex).intValue();
    } else {
      throw new StageException(Errors.YAML_UPGRADER_14, matchIndex);
    }
    return this;
  }

  public Object getElseNewValue() {
    return elseNewValue;
  }

  public ReplaceConfigUpgraderAction setElseNewValue(Object elseNewValue) {
    this.elseNewValue = elseNewValue;
    return this;
  }

  protected Object getNewValue(Map<String, Object> originalConfigs, Object oldValue, Object newValue) {
    Object value = newValue;
    if (newValue instanceof String) {
      String newValueStr = (String) newValue;
      if (newValueStr.contains(tokenForOldValue)) {
        if (oldValue instanceof String) {
          value = newValueStr.replace(tokenForOldValue, (String) oldValue);
        } else {
          throw new StageException(Errors.YAML_UPGRADER_05, getName());
        }
      }
      value = resolveValueIfEL(originalConfigs, value);
    }
    return value;
  }

  protected Object getNewValueFromMatchIndex(ConfigsAdapter.Pair config) {
    Pattern pattern = Pattern.compile(getIfOldValueMatches().toString());
    Matcher match = pattern.matcher(config.getValue().toString());
    if (match.matches()) {
      try {
        newValue = match.group(newValueFromMatchIndex);
      } catch (IndexOutOfBoundsException e) {
        return elseNewValue;
      }
      return newValue;
    }
    return elseNewValue;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    Utils.checkNotNull(getName(), "name");
    if (newValue != null && newValueFromMatchIndex != null) {
      throw new StageException(Errors.YAML_UPGRADER_15);
    } else if (newValue == null && newValueFromMatchIndex == null) {
      throw new StageException(Errors.YAML_UPGRADER_16);
    }

    ConfigsAdapter configsAdapter = wrap(configs);
    ConfigsAdapter.Pair config = configsAdapter.find(getName());
    if (config != null) {
      if (MATCHES_ALL.equals(getIfOldValueMatches())) {
        configsAdapter.set(getName(), getNewValue(originalConfigs, config.getValue(), getNewValue()));
      } else {
        boolean matches;
        if ((config.getValue() instanceof String)) {
          Pattern pattern = Pattern.compile(getIfOldValueMatches().toString());
          matches = pattern.matcher(config.getValue().toString()).matches();
        } else {
          matches = config.getValue().equals(getIfOldValueMatches());
        }
        if (matches && newValueFromMatchIndex == null) {
          configsAdapter.set(getName(), getNewValue(originalConfigs, config.getValue(), getNewValue()));
        } else if (matches && newValueFromMatchIndex != null){
          configsAdapter.set(getName(), getNewValueFromMatchIndex(config));
        } else if (getElseNewValue() != null) {
          configsAdapter.set(getName(), getNewValue(originalConfigs, config.getValue(), getElseNewValue()));
        }
      }
    }
  }

}
