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
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class TestFieldRenamerProcessorUpgrader {
  private static final String RENAME_MAPPING = "renameMapping";
  private static final String OVERWRITE_EXISTING = "overwriteExisting";
  private static final Joiner JOINER = Joiner.on(".");
  private static final String ERROR_HANDLER = "errorHandler";
  private static final String EXISTING_FIELD_HANDLING = ERROR_HANDLER + ".existingToFieldHandling";
  private static final String ONSTAGE_PRECONDITION_FAILURE = "onStagePreConditionFailure";
  private static final String NON_EXISTING_FROM_FIELD_HANDLING = ERROR_HANDLER + ".nonExistingFromFieldHandling";
  private static final String MULTIPLE_FROM_FIELDS_MATHCHING = ERROR_HANDLER + ".multipleFromFieldsMatching";


  private void checkRenameMapping(Config config) {
    if (config.getName().equals(RENAME_MAPPING)) {
      List<LinkedHashMap<String, Object>> upgradedRenameMappingList = (List<LinkedHashMap<String, Object>>) config.getValue();
      Assert.assertEquals("Only one mapping should be present", 1, upgradedRenameMappingList.size());
      LinkedHashMap<String, Object> upgradedRenameMapping = upgradedRenameMappingList.get(0);
      Assert.assertEquals("There should be two properties inside upgraded rename mapping", 2, upgradedRenameMapping.size());
      Assert.assertTrue("fromFieldExpression should be present", upgradedRenameMapping.containsKey("fromFieldExpression"));
      Assert.assertTrue("toFieldExpression should be present", upgradedRenameMapping.containsKey("toFieldExpression"));
      Assert.assertEquals("From Field Expression should be /a", "/a", upgradedRenameMapping.get("fromFieldExpression"));
      Assert.assertEquals("To Field Expression should be /a", "/b", upgradedRenameMapping.get("toFieldExpression"));
    }
  }

  @Test
  public void testUpgradeV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();
    List<LinkedHashMap<String, Object>> renameMappingList = new ArrayList<>();
    LinkedHashMap<String, Object> renameMapping = new LinkedHashMap<String, Object>();
    renameMapping.put("fromField", "/a");
    renameMapping.put("toField", "/b");
    renameMappingList.add(renameMapping);
    configs.add(new Config(RENAME_MAPPING, renameMappingList));
    configs.add(new Config(OVERWRITE_EXISTING, false));
    configs.add(new Config(ONSTAGE_PRECONDITION_FAILURE, OnStagePreConditionFailure.CONTINUE));

    FieldRenamerProcessorUpgrader upgrader = new FieldRenamerProcessorUpgrader();
    upgrader.upgrade("a", "b", "c", 1, 2, configs);

    Assert.assertEquals("Only one config", 4, configs.size());
    for (Config config : configs) {
      switch (config.getName()) {
        case RENAME_MAPPING:
          checkRenameMapping(config);
          break;
        case EXISTING_FIELD_HANDLING:
          Assert.assertEquals(
              "Existing To Field Handling should be TO_ERROR, if overwriteExisting was false",
              ExistingToFieldHandling.TO_ERROR,
              config.getValue()
          );
          break;
        case MULTIPLE_FROM_FIELDS_MATHCHING:
          Assert.assertEquals(
              "Multiple from Fields Matching Regex should be TO_ERROR",
              OnStagePreConditionFailure.TO_ERROR,
              config.getValue()
          );
          break;
        case NON_EXISTING_FROM_FIELD_HANDLING:
          Assert.assertEquals(
              "Non Existing Field Handling should be CONTINUE, if  OnStagePreConditionFailure.CONTINUE",
              OnStagePreConditionFailure.CONTINUE,
              config.getValue()
          );
          break;
      }

    }
  }
}
