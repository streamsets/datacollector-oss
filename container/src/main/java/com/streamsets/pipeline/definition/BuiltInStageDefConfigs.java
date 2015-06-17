/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.OnRecordErrorChooserValues;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.config.PipelineGroups;

import java.util.List;

@GenerateResourceBundle
@ConfigGroups(PipelineGroups.class)
public abstract class BuiltInStageDefConfigs implements Stage {

  public static final String STAGE_ON_RECORD_ERROR_CONFIG = "stageOnRecordError";
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TO_ERROR",
      label = "On Record Error",
      description = "Action to take with records sent to error",
      displayPosition = 10
  )
  @ValueChooser(OnRecordErrorChooserValues.class)
  public OnRecordError stageOnRecordError;

  public static final String STAGE_REQUIRED_FIELDS_CONFIG = "stageRequiredFields";
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      label = "Required Fields",
      description = "Records without any of these fields are sent to error",
      displayPosition = 20
  )
  @FieldSelector(singleValued = false)
  public List<String> stageRequiredFields;

  public static final String STAGE_PRECONDITIONS_CONFIG = "stageRecordPreconditions";
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.LIST,
      label = "Preconditions",
      description = "Records that don't satisfy all the preconditions are sent to error",
      displayPosition = 30,
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public List<String> stageRecordPreconditions;

}
