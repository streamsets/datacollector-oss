/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;


import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.OnRecordErrorChooserValues;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.util.List;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public class StageConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "On Record Error",
      description = "Action to take with records sent to error",
      displayPosition = 30,
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
      displayPosition = 10,
      group = ""
  )
  @FieldSelector
  public List<String> stageRequiredFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue="",
      label = "Preconditions",
      description = "Records that don't satisfy all the preconditions are sent to error",
      displayPosition = 20,
      group = "",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      elDefs = { RecordEL.class, StringEL.class, RuntimeEL.class }
  )
  public List<String> stageRecordPreconditions;

}
