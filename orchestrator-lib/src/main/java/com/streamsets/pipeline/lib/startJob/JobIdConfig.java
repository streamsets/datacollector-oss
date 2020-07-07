/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.startJob;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

public class JobIdConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Identifier Type",
      defaultValue = "ID",
      displayPosition = 15,
      group = "JOB"
  )
  @ValueChooserModel(JobIdTypeChooserValues.class)
  public JobIdType jobIdType = JobIdType.ID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Identifier",
      description = "ID or name of the job to start",
      displayPosition = 20,
      group = "JOB",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String jobId = "";

  @ConfigDef(
      required = false,
      defaultValue = "{}",
      type = ConfigDef.Type.TEXT,
      label = "Runtime Parameters",
      description = "Runtime parameters to pass to the job",
      displayPosition = 30,
      group = "JOB",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String runtimeParameters;
}
