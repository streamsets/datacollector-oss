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
package com.streamsets.pipeline.stage.metadata.gen2;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;
import com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor.HdfsActionsConfig;

@StageDef(
  version = 2,
  label = "ADLS Gen2 File Metadata",
  description = "Changes ADLS Gen2 file metadata, such as renaming files or changing permissions",
  icon = "data-lake-metadata-executor-gen2.png",
  privateClassLoader = true,
  upgrader = DataLakeGen2MetadataUpgrader.class,
  onlineHelpRefUrl ="index.html?contextID=task_uwz_m45_rhb",
  producesEvents = true
)
@ConfigGroups(value = DataLakeGen2MetadataGroups.class)
@HideConfigs(value = {
    "connection.hdfsUri",
    "connection.hdfsUser",
    "connection.hdfsKerberos",
    "connection.hdfsConfDir",
    "connection.hdfsConfigs"
})
@PipelineLifecycleStage
@GenerateResourceBundle
public class DataLakeGen2MetadataDExecutor extends DExecutor {

  @ConfigDefBean
  public DataLakeGen2ConnectionConfig connection;

  @ConfigDefBean
  public HdfsActionsConfig  actions;

  @Override
  protected Executor createExecutor() {
    return new DataLakeGen2MetadataExecutor(connection, actions);
  }
}
