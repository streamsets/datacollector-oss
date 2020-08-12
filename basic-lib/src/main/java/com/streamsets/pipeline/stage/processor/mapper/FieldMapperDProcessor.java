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
package com.streamsets.pipeline.stage.processor.mapper;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version=1,
    label="Field Mapper",
    description="Maps fields in records based on expressions. Operates on field paths, names, or values.",
    // from https://www.iconfinder.com/icons/1243687/guide_map_navigation_icon
    icon="iconfinder_thefreeforty_map_1243687.png",
    upgrader = FieldMapperProcessorUpgrader.class,
    upgraderDef = "upgrader/FieldMapperDProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_f2j_g2q_xgb",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EMR_BATCH
    }
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldMapperDProcessor extends DProcessor {

  @ConfigDefBean
  public FieldMapperProcessorConfig fieldMapperConfig;

  @Override
  protected Processor createProcessor() {
    return new FieldMapperProcessor(fieldMapperConfig);
  }

}
