/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;


import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.OnRecordErrorChooserValues;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooser;

import java.util.List;

//Dummy stage that is used to produce the resource bundle for system driven configs
//
// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public abstract class SystemStageConfigs implements Stage {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "On Record Error",
      description = "Action to take with records sent to error",
      displayPosition = 0,
      group = ""
  )
  @ValueChooser(OnRecordErrorChooserValues.class)
  public OnRecordError stageOnRecordError;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Required Fields",
      description = "Records without any of these fields are sent to error",
      displayPosition = 0,
      group = ""
  )
  @FieldSelector
  public List<String> stageRequiredFields;

}
