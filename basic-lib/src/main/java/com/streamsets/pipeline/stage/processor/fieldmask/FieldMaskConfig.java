/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldmask;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.List;

public class FieldMaskConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Mask",
      description="Mask string fields. You can enter multiple fields for the same mask type.",
      displayPosition = 10
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="VARIABLE_LENGTH",
      label = "Mask Type",
      description="",
      displayPosition = 20
  )
  @ValueChooserModel(MaskTypeChooseValues.class)
  public MaskType maskType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Custom Mask",
      description = "Use # to reveal field values. Other characters replace field values.",
      displayPosition = 30,
      dependsOn = "maskType",
      triggeredByValue = "CUSTOM"
  )
  public String mask;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Regular Expression",
    description = "Regular expression that matches the data and groups them.",
    displayPosition = 40,
    dependsOn = "maskType",
    triggeredByValue = "REGEX",
    defaultValue = "(.*)"

  )
  public String regex;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    label = "Groups To Show",
    description = "Comma separated list of group numbers that must be revealed in the data.",
    displayPosition = 50,
    dependsOn = "maskType",
    triggeredByValue = "REGEX",
    defaultValue = "1"
  )
  public String groupsToShow;

}
