/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.selector;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.LanePredicateMapping;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;
import java.util.Map;

@StageDef(
    version = "1.0.0",
    label = "Stream Selector",
    description = "Passes records to streams based on conditions",
    icon="laneSelector.png",
    outputStreams = StageDef.VariableOutputStreams.class,
    outputStreamsDrivenByConfig = "lanePredicates")
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SelectorDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Condition",
      description = "Records that match the condition pass to the stream",
      displayPosition = 10,
      group = "CONDITIONS"
  )
  @LanePredicateMapping
  public List<Map<String, String>> lanePredicates;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MAP,
      label = "Constants",
      description = "Can be used in any expression in the processor",
      displayPosition = 20,
      group = "CONDITIONS"
  )
  public Map<String, ?> constants;

  @Override
  protected Processor createProcessor() {
    return new SelectorProcessor(lanePredicates, constants);
  }

}
