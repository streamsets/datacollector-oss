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
package com.streamsets.pipeline.stage.destination.remote;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.event.WholeFileProcessedEvent;

@StageDef(
    version = 3,
    label = "SFTP/FTP/FTPS Client",
    description = "Uses an SFTP/FTP/FTPS client to send data to a URL.",
    icon = "sftp-client.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {WholeFileProcessedEvent.class},
    upgraderDef = "upgrader/RemoteUploadDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_jgs_4fw_pgb"
)
@HideConfigs(value = {
    "conf.dataFormatConfig.includeChecksumInTheEvents",
    "conf.remoteConfig.disableReadAheadStream"
})
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class RemoteUploadDTarget extends DTarget {

  @ConfigDefBean
  public RemoteUploadConfigBean conf;

  @Override
  protected Target createTarget() {
    return new RemoteUploadTarget(conf);
  }
}
