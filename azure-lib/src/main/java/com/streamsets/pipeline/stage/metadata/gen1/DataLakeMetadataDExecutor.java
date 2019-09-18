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
package com.streamsets.pipeline.stage.metadata.gen1;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;
import com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor.HdfsActionsConfig;
import com.streamsets.pipeline.stage.metadata.DataLakeMetadataGroups;

@StageDef(
  version = 1,
  label = "ADLS Gen1 File Metadata",
  description = "Changes ADLS Gen1 file metadata, such as renaming files or changing permissions",
  icon = "data-lake-metadata-executor-gen1.png",
  privateClassLoader = true,
  upgraderDef = "upgrader/DataLakeMetadataDExecutor.yaml",
  onlineHelpRefUrl ="index.html?contextID=task_vpx_ys5_5hb",
  producesEvents = true
)
@ConfigGroups(value = DataLakeMetadataGroups.class)
@HideConfigs(value = {
    "connection.hdfsUri",
    "connection.hdfsUser",
    "connection.hdfsKerberos",
    "connection.hdfsConfDir",
    "connection.hdfsConfigs"
})
@PipelineLifecycleStage
@GenerateResourceBundle
public class DataLakeMetadataDExecutor extends DExecutor {

  @ConfigDefBean
  public DataLakeConnectionConfig connection;

  @ConfigDefBean
  public HdfsActionsConfig  actions;

  @Override
  protected Executor createExecutor() {
    return new DataLakeMetadataExecutor(connection, actions);
  }
}
