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

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@StageDef(
    version = 1,
    label = "To Error",
    description = "Sends records to the pipeline configured error records handling",
    icon="toerror.png",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Destinations/ToError.html"
)
@HideConfigs(preconditions = true, onErrorRecord = true)
@GenerateResourceBundle
public class ToErrorDTarget extends DTarget {

  @Override
  protected Target createTarget() {
    return new ToErrorTarget();
  }
}
