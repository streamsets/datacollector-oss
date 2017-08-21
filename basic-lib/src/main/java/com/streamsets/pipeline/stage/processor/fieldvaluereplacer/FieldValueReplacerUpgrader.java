/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.fieldvaluereplacer;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class FieldValueReplacerUpgrader implements StageUpgrader {
  static final String NULL_REPLACER_CONFIGS = "nullReplacerConditionalConfigs";
  static final String FIELDS_TO_NULL = "fieldsToNull";
  static final String  FIELDS_TO_CONDITIONALLY_REPLACE= "fieldsToConditionallyReplace";
  static final String NULL_REPLACER_CONFIG_CONDITION = "condition";

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    //This property was added after 2.1, but i have a hunch we will kind of refactor  this property (may be make this an EL)
    //a bit in future.
    //Nevertheless adding an explicit upgrade step, so if we rename/change this property our upgrade can gracefully handle it.
    boolean shouldAdd = true;
    for (Config config : configs) {
      if (config.getName().equals(FIELDS_TO_CONDITIONALLY_REPLACE) && config.getValue() != null) {
        shouldAdd = false;
      }
    }
    //Only add the property if it is not there in the definition
    if (shouldAdd) {
      configs.add(new Config(FIELDS_TO_CONDITIONALLY_REPLACE, new ArrayList<LinkedHashMap<String, Object>>()));
    }
  }

  @SuppressWarnings("unchecked")
  private void upgradeV2ToV3(List<Config> configs) {
    List<Config> configsToAdd = new ArrayList<>();
    List<Config> configsToRemove = new ArrayList<>();
    for (Config config : configs) {
      if (config.getName().equals(FIELDS_TO_NULL) && config.getValue() != null) {
        List<String> fieldsToNull = (List<String>)config.getValue();
        List<LinkedHashMap<String, Object>> nullFieldValueReplacerConfigs = new ArrayList<>();

        LinkedHashMap<String, Object> nullFieldValueReplacerConfig = new LinkedHashMap<>();
        nullFieldValueReplacerConfig.put(FIELDS_TO_NULL, fieldsToNull);
        nullFieldValueReplacerConfig.put(NULL_REPLACER_CONFIG_CONDITION, "");

        nullFieldValueReplacerConfigs.add(nullFieldValueReplacerConfig);

        configsToAdd.add(new Config(NULL_REPLACER_CONFIGS, nullFieldValueReplacerConfigs));
        configsToRemove.add(config);
      }
    }
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
