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
package com.streamsets.pipeline.stage.processor.dedup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version = 1,
    label = "Record Deduplicator",
    description = "Separates unique and duplicate records based on field comparison",
    icon="dedup.png",
    outputStreams = OutputStreams.class,
    execution = ExecutionMode.STANDALONE,
    flags = StageBehaviorFlags.PASSTHROUGH,
    upgraderDef = "upgrader/DeDupDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_ikr_c2f_zq"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class DeDupDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000000",
      label = "Max Records to Compare",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DE_DUP",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int recordCountWindow;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Time to Compare (secs)",
      description = "Creates a window of time for comparison. Takes precedence over Max Records. Use 0 for no time window.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DE_DUP",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int timeWindowSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ALL_FIELDS",
      label = "Compare",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DE_DUP"
  )
  @ValueChooserModel(SelectFieldsChooserValues.class)
  public SelectFields compareFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Fields to Compare",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DE_DUP",
      dependsOn = "compareFields",
      triggeredByValue = "SPECIFIED_FIELDS"
  )
  @FieldSelectorModel
  public List<String> fieldsToCompare;

  @Override
  protected Processor createProcessor() {
    return new DeDupProcessor(recordCountWindow, timeWindowSecs, compareFields, fieldsToCompare);
  }

}
