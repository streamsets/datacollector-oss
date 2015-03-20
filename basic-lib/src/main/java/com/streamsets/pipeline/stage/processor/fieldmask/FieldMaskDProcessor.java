/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version = "1.0.0",
    label = "Field Masker",
    description = "Masks field values",
    icon = "mask.png"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldMaskDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "",
      description = "",
      displayPosition = 10,
      group = "MASKING"
  )
  @ComplexField
  public List<FieldMaskConfig> fieldMaskConfigs;

  @Override
  protected Processor createProcessor() {
    return new FieldMaskProcessor(fieldMaskConfigs);
  }

}
