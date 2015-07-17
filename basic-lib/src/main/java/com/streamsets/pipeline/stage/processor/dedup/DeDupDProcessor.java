/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.dedup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfig;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version = 1,
    label = "Record Deduplicator",
    description = "Separates unique and duplicate records based on field comparison",
    icon="dedup.png",
    outputStreams = OutputStreams.class,
    execution = ExecutionMode.STANDALONE
)
@ConfigGroups(Groups.class)
@HideConfig(onErrorRecord = true)
@GenerateResourceBundle
public class DeDupDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000000",
      label = "Max Records to Compare",
      displayPosition = 10,
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
      group = "DE_DUP"
  )
  @ValueChooser(SelectFieldsChooserValues.class)
  public SelectFields compareFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Fields to Compare",
      displayPosition = 40,
      group = "DE_DUP",
      dependsOn = "compareFields",
      triggeredByValue = "SPECIFIED_FIELDS"
  )
  @FieldSelector
  public List<String> fieldsToCompare;

  @Override
  protected Processor createProcessor() {
    return new DeDupProcessor(recordCountWindow, timeWindowSecs, compareFields, fieldsToCompare);
  }

}
