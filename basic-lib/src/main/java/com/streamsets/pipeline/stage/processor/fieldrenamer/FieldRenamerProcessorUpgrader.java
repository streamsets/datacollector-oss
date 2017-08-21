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
package com.streamsets.pipeline.stage.processor.fieldrenamer;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public final class FieldRenamerProcessorUpgrader implements StageUpgrader{
  private static final Joiner JOINER = Joiner.on(".");
  private static final String FIELD_RENAMER_CONFIGS = "renameMapping";
  private static final String FROM_FIELD = "fromField";
  private static final String TO_FIELD = "toField";
  private static final String FROM_FIELD_EXP = "fromFieldExpression";
  private static final String TO_FIELD_EXP = "toFieldExpression";
  private static final String OVERWRITE_EXISTING = "overwriteExisting";
  private static final String EXISTING_FIELD_HANDLING = "existingToFieldHandling";
  private static final String ONSTAGE_PRECONDITION_FAILURE = "onStagePreConditionFailure";
  private static final String NON_EXISTING_FROM_FIELD_HANDLING = "nonExistingFromFieldHandling";
  private static final String MULTIPLE_FROM_FIELDS_MATHCHING = "multipleFromFieldsMatching";
  private static final String ERROR_HANDLER = "errorHandler";

  @Override
  public List<Config> upgrade (
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToAdd = new ArrayList<>();
    List<Config> configsToRemove = new ArrayList<>();
    for (Config config : configs) {
      switch (config.getName()) {
        case FIELD_RENAMER_CONFIGS:
          List<LinkedHashMap<String, Object>> renameMapping = (List<LinkedHashMap<String, Object>>) config.getValue();
          Iterator<LinkedHashMap<String, Object>> renamingMappingIterator = renameMapping.iterator();
          while (renamingMappingIterator.hasNext()) {
            LinkedHashMap<String, Object> renameMappingEntry = renamingMappingIterator.next();
            renameMappingEntry.put(FROM_FIELD_EXP, renameMappingEntry.get(FROM_FIELD));
            renameMappingEntry.remove(FROM_FIELD);
            renameMappingEntry.put(TO_FIELD_EXP, renameMappingEntry.get(TO_FIELD));
            renameMappingEntry.remove(TO_FIELD);
          }
          break;
        case OVERWRITE_EXISTING:
          configsToRemove.add(config);
          ExistingToFieldHandling existingToFieldHandling = (boolean)(config.getValue())?
              ExistingToFieldHandling.REPLACE : ExistingToFieldHandling.TO_ERROR;
          configsToAdd.add(new Config(JOINER.join(ERROR_HANDLER, EXISTING_FIELD_HANDLING), existingToFieldHandling));
          break;
        case ONSTAGE_PRECONDITION_FAILURE:
          configsToRemove.add(config);
          configsToAdd.add(new Config(JOINER.join(ERROR_HANDLER, NON_EXISTING_FROM_FIELD_HANDLING), config.getValue()));
          break;
      }
    }
    configsToAdd.add(
        new Config(
            JOINER.join(ERROR_HANDLER, MULTIPLE_FROM_FIELDS_MATHCHING),
            OnStagePreConditionFailure.TO_ERROR
        )
    );
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
