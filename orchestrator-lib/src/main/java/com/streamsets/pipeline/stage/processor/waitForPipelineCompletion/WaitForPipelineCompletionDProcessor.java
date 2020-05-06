/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.waitForPipelineCompletion;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.startPipeline.Groups;

@StageDef(
    version = 1,
    label = "Wait for Pipeline Completion",
    description = "Wait for a Data Collector, Transformer, or Edge pipeline to complete",
    icon="pipeline.png",
    execution = {
        ExecutionMode.STANDALONE
    },
    onlineHelpRefUrl ="index.html?contextID=task_whc_d43_plb"
)
@GenerateResourceBundle
@HideConfigs({
    "conf.tlsConfig.keyStoreFilePath",
    "conf.tlsConfig.keyStoreType",
    "conf.tlsConfig.keyStorePassword",
    "conf.tlsConfig.keyStoreAlgorithm"
})
@ConfigGroups(Groups.class)
public class WaitForPipelineCompletionDProcessor extends DProcessor {

  @ConfigDefBean
  public WaitForPipelineCompletionConfig conf;

  @Override
  protected Processor createProcessor() {
    return new WaitForPipelineCompletionProcessor(conf);
  }

}
