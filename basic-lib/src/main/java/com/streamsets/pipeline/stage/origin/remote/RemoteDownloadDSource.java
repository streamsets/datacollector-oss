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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.lib.event.FinishedFileEvent;
import com.streamsets.pipeline.lib.event.NewFileEvent;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.lib.util.SystemClock;

@StageDef(
    version = 8,
    label = "SFTP/FTP/FTPS Client",
    description = "Uses an SFTP/FTP/FTPS client to read data from a URL.",
    icon = "sftp-client.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {NewFileEvent.class, FinishedFileEvent.class, NoMoreDataEvent.class},
    upgrader = RemoteDownloadSourceUpgrader.class,
    upgraderDef = "upgrader/RemoteDownloadDSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_lfx_fzd_5v"
)
@HideConfigs(value = {"conf.dataFormatConfig.verifyChecksum", "conf.remoteConfig.createPathIfNotExists"})
@GenerateResourceBundle
@ConfigGroups(Groups.class)
public class RemoteDownloadDSource extends DSource {

  @ConfigDefBean
  public RemoteDownloadConfigBean conf;

  @Override
  protected Source createSource() {
    return new RemoteDownloadSource(
        conf,
        new FileDelayer(new SystemClock(), conf.processingDelay)
    );
  }
}
