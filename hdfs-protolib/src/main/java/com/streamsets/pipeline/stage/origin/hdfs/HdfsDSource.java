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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DPushSource;

@StageDef(
    version = 1,
    label = "Hadoop FS",
    description = "Reads from a Hadoop compatible file system",
    icon = "hdfs.png",
    execution = ExecutionMode.STANDALONE,
    resetOffset = true,
    producesEvents = true,
    privateClassLoader = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {"hdfsSourceConfigBean.hdfsKerberos", "hdfsSourceConfigBean.hdfsUser"})
@GenerateResourceBundle
public class HdfsDSource extends DPushSource {

  @ConfigDefBean
  public HdfsSourceConfigBean hdfsSourceConfigBean;

  @Override
  protected PushSource createPushSource() {
    return new HdfsSource(hdfsSourceConfigBean);
  }


}
