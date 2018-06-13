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
package com.streamsets.pipeline.stage.destination.eventhubs;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    // We're reusing upgrader for both ToErrorEventHubProducerDTarget and EventHubProducerDTarget, make sure that you
    // upgrade both versions at the same time when changing.
    version = 1,
    label = "Write to Azure Event Hub",
    description = "",
    icon = "",
    recordsByRef = true,
    onlineHelpRefUrl ="index.html?contextID=task_in4_f5q_1bb"
)
@ErrorStage
@HideConfigs(preconditions = true, onErrorRecord = true, value = {
    "producerConf.dataFormat",
})
@GenerateResourceBundle
public class ToErrorEventHubProducerDTarget extends EventHubProducerDTarget {
  @Override
  protected Target createTarget() {
    producerConf.dataFormat = DataFormat.SDC_JSON;
    return new EventHubProducerTarget(commonConf, producerConf);
  }
}
