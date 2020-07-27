/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.finishpipeline.config;

import com.streamsets.pipeline.api.ConfigDef;

public class PipelineFinisherConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    label = "Reset Origin",
    defaultValue = "false",
    description = "Resets the origin after the executor stops the pipeline. When enabled, the origin processes all available data each time the pipeline runs.",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC,
    group = "FINISHER"
  )
  public boolean resetOffset = false;
}
