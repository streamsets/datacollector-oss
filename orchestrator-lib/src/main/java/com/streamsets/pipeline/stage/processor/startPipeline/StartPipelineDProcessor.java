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
package com.streamsets.pipeline.stage.processor.startPipeline;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.startPipeline.Groups;
import com.streamsets.pipeline.lib.startPipeline.StartPipelineConfig;

@StageDef(
    version = 3,
    label = "Start Pipelines",
    description = "Starts pipelines on Data Collector, Edge, or Transformer",
    icon="pipeline.png",
    execution = {
        ExecutionMode.STANDALONE
    },
    onlineHelpRefUrl ="index.html?contextID=task_yh1_wxr_2jb",
    upgrader = StartPipelineDProcessorUpgrader.class,
    upgraderDef = "upgrader/StartPipelineDProcessor.yaml"
)
@GenerateResourceBundle
@HideConfigs({
    "conf.tlsConfig.useRemoteKeyStore",
    "conf.tlsConfig.keyStoreFilePath",
    "conf.tlsConfig.privateKey",
    "conf.tlsConfig.certificateChain",
    "conf.tlsConfig.keyStoreType",
    "conf.tlsConfig.keyStorePassword",
    "conf.tlsConfig.keyStoreAlgorithm"
})
@ConfigGroups(Groups.class)
public class StartPipelineDProcessor extends DProcessor {

  @ConfigDefBean
  public StartPipelineConfig conf;

  @Override
  protected Processor createProcessor() {
    return new StartPipelineProcessor(conf);
  }

}
