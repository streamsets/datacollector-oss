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

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegisterServiceAction<T> extends UpgraderAction<RegisterServiceAction, T> {
  private String service;
  private String existingConfigsPattern;

  public RegisterServiceAction(Function<T, ConfigsAdapter> wrapper) {
    super(wrapper);
  }

  public void setService(String service) {
    this.service = service;
  }

  public void setExistingConfigsPattern(String existingConfigsPattern) {
    this.existingConfigsPattern = existingConfigsPattern;
  }

  @Override
  public void upgrade(
      StageUpgrader.Context context,
      Map<String, Object> originalConfigs,
      T configs
  ) {
    List<Config> serviceConfigs = new ArrayList<>();

    if (existingConfigsPattern !=null && !existingConfigsPattern.isEmpty()) {
      Pattern pattern = Pattern.compile(existingConfigsPattern);
      ConfigsAdapter configsAdapter = wrap(configs);
      Iterator<ConfigsAdapter.Pair> iterator = configsAdapter.iterator();
      while (iterator.hasNext()) {
        ConfigsAdapter.Pair config = iterator.next();
        Matcher matcher = pattern.matcher(config.getName());
        if (matcher.matches()) {
          ConfigsAdapter.Pair pair = configsAdapter.remove(config.getName());
          serviceConfigs.add(new Config(matcher.group(1), pair.getValue()));
        }
      }
    }
    try {
      // For now we use upgrade context to register the service class,
      // Later we should allow for passing service name/version to be registered
      context.registerService(
          Thread.currentThread().getContextClassLoader().loadClass(service),
          serviceConfigs
      );
    } catch (ClassNotFoundException e) {
      throw new StageException(Errors.YAML_UPGRADER_12, service);
    }
  }
}
