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
package com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

@StageDef(
  version = 1,
  label = "HDFS File Metadata",
  description = "Changes HDFS file metadata such as renaming files or changing permissions.",
  icon = "hdfs-executor.png",
  privateClassLoader = true,
  upgraderDef = "upgrader/HdfsMetadataDExecutor.yaml",
  onlineHelpRefUrl ="index.html?contextID=task_m3v_5lk_fx",
  producesEvents = true
)
@ConfigGroups(value = Groups.class)
@PipelineLifecycleStage
@GenerateResourceBundle
public class HdfsMetadataDExecutor extends DExecutor {

  @ConfigDefBean
  public HdfsConnectionConfig connection;

  @ConfigDefBean
  public HdfsActionsConfig  actions;

  @Override
  protected Executor createExecutor() {
    return new HdfsMetadataExecutor(connection, actions);
  }
}
