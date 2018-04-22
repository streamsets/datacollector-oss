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

import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Pipeline Finisher Executor",
    description = "Forces pipeline to transition to Finished after receiving an event.",
    icon = "finisher.png",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Executors/PipelineFinisher.html#task_lrm_pws_3z"
)

public class PipelineFinisherDExecutor extends DExecutor {

  @Override
  protected Executor createExecutor() {
    return new PipelineFinisherExecutor();
  }

}
