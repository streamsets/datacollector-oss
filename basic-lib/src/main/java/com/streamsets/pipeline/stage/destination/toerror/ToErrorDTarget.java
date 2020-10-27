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
package com.streamsets.pipeline.stage.destination.toerror;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.el.RecordEL;

@StageDef(
    version = 2,
    label = "To Error",
    description = "Sends records to the pipeline configured error records handling",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH
    },
    icon="toerror.png",
    flags = StageBehaviorFlags.PURE_FUNCTION,
    upgraderDef = "upgrader/ToErrorDTarget.yaml",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Destinations/ToError.html"
)
@HideConfigs(preconditions = true, onErrorRecord = true)
@GenerateResourceBundle
public class ToErrorDTarget extends DTarget {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Stop Pipeline on Error",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED
  )
  public boolean stopPipelineOnError;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Error Message",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      defaultValue = "Sent to error by previous stage",
      dependsOn = "stopPipelineOnError",
      triggeredByValue = "true"
  )
  public String errorMessage = Errors.TOERROR_00.getMessage();

  @Override
  protected Target createTarget() {
    return new ToErrorTarget(stopPipelineOnError, errorMessage);
  }
}
