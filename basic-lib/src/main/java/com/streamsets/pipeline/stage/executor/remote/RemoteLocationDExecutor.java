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
package com.streamsets.pipeline.stage.executor.remote;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

@StageDef(
    version = 3,
    label = "SFTP/FTP/FTPS Client",
    description = "Run event based task on SFTP/FTP/FTPS Client specified on URL",
    icon = "sftp-client.png",
    execution = ExecutionMode.STANDALONE,
    upgraderDef = "upgrader/RemoteLocationDExecutor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_s1r_vjd_hlb"
)
@HideConfigs(value = {
    "conf.remoteConfig.disableReadAheadStream",
    "conf.remoteConfig.createPathIfNotExists"
})

@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class RemoteLocationDExecutor extends DExecutor {

  @ConfigDefBean
  public RemoteExecutorConfigBean conf;

  @Override
  protected Executor createExecutor() {
    return new RemoteLocationExecutor(conf);
  }
}
