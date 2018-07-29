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
package com.streamsets.pipeline.stage.processor.fuzzy;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.List;

// Hide this processor from users until its fate is sealed.
//@StageDef(
//    version = 1,
//    label = "Fuzzy Field Replacer",
//    description = "Canonicalizes Field Names based on fuzzy matching.",
//    icon = "replacer.png"
//)
@ConfigGroups(Groups.class)
//@GenerateResourceBundle
public class FuzzyFieldDProcessor extends DProcessor {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Field Paths",
      description = "Field paths to match against desired field names up to 1 level deep.",
      displayPosition = 10,
      group = "FUZZY"
  )
  @FieldSelectorModel
  public List<String> rootFieldPaths;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "",
      label = "Desired Field Names",
      description = "Desired output fields are matched to all input fields.",
      displayPosition = 20,
      group = "FUZZY"
  )
  public List<String> outputFieldNames;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      min = 0,
      max = 100,
      label = "Match Threshold",
      description = "Minimum match score to accept.",
      displayPosition = 30,
      group = "FUZZY"
  )
  public int matchThreshold;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Output All Candidates",
      description = "If checked, outputs all fields meeting the threshold, instead of only the best match.",
      displayPosition = 40,
      group = "FUZZY"
  )
  public boolean allCandidates;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      dependsOn = "allCandidates",
      triggeredByValue = "false",
      label = "In-place Replacement",
      description = "If checked, does not output candidate information, simply replaces the header in-place.",
      displayPosition = 50,
      group = "FUZZY"
  )
  public boolean inPlace;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Preserve Unmatched Fields",
      description = "If checked, preserves fields which did not match any desired field name.",
      displayPosition = 50,
      group = "FUZZY"
  )
  public boolean preserveUnmatchedFields;

  @Override
  protected Processor createProcessor() {
    return new FuzzyFieldProcessor(
        rootFieldPaths,
        outputFieldNames,
        matchThreshold,
        allCandidates,
        inPlace,
        preserveUnmatchedFields
    );
  }
}
