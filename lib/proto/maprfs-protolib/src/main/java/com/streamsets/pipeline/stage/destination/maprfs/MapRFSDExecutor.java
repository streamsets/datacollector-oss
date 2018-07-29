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
package com.streamsets.pipeline.stage.destination.maprfs;

import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.stage.destination.hdfs.metadataexecutor.HdfsMetadataDExecutor;

@StageDef(
    version = 1,
    label = "MapR FS File Metadata",
    description = "Changes MapR FS file metadata such as renaming files or changing permissions.",
    icon = "mapr_xd.png",
    producesEvents = true,
    privateClassLoader = false,
    onlineHelpRefUrl ="index.html?contextID=task_v3c_bvh_z1b"
)
@GenerateResourceBundle
@PipelineLifecycleStage
public class MapRFSDExecutor extends HdfsMetadataDExecutor {

  @Override
  protected Executor createExecutor() {
    // Since we're inheriting the configuration from usual HDFS executor, we don't have a way to override the default value in the
    // annotation and hence the default value is kind of "hidden" and supplied here.
    if(connection.hdfsUri == null || connection.hdfsUri.isEmpty()) {
      connection.hdfsUri = "maprfs:///";
    }

    return super.createExecutor();
  }

}
