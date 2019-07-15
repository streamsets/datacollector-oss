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
package com.streamsets.pipeline.stage.executor.finishpipeline;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;
import com.streamsets.pipeline.stage.executor.finishpipeline.config.PipelineFinisherConfig;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Pipeline Finisher Executor",
    description = "Forces pipeline to transition to Finished after receiving an event.",
    icon = "finisher.png",
    flags = StageBehaviorFlags.PASSTHROUGH,
    upgraderDef = "upgrader/PipelineFinisherDExecutor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_lrm_pws_3z"
)
@ConfigGroups(Groups.class)
public class PipelineFinisherDExecutor extends DExecutor {

  @ConfigDefBean public PipelineFinisherConfig config = new PipelineFinisherConfig();

  @Override
  protected Executor createExecutor() {
    return new PipelineFinisherExecutor(config);
  }

}
