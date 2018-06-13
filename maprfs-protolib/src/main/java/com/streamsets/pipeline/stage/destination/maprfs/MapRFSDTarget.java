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

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;

@StageDef(
    version = 2,
    label = "MapR FS",
    description = "Writes to a MapR filesystem",
    icon = "mapr_xd.png",
    producesEvents = true,
    privateClassLoader = false,
    upgrader = MapRFSTargetUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_spl_1fj_fv"
)
@HideConfigs(
    value = {
        "hdfsTargetConfigBean.dataGeneratorFormatConfig.includeSchema",
    }
)
@GenerateResourceBundle
public class MapRFSDTarget extends HdfsDTarget {
  @Override
  protected Target createTarget() {
    // Since we're inheriting the configuration from usual HDFS target, we don't have a way to override the default value in the
    // annotation and hence the default value is kind of "hidden" and supplied here.
    if(hdfsTargetConfigBean.hdfsUri == null || hdfsTargetConfigBean.hdfsUri.isEmpty()) {
      hdfsTargetConfigBean.hdfsUri = "maprfs:///";
    }

    return super.createTarget();
  }
}
