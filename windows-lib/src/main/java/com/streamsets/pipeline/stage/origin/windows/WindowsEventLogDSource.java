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
package com.streamsets.pipeline.stage.origin.windows;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.stage.origin.windows.wineventlog.WinEventLogConfigBean;

@GenerateResourceBundle
@StageDef(
    version = 3,
    label = "Windows Event Log",
    description = "Reads data from a Windows event log",
    execution = {ExecutionMode.EDGE},
    icon = "winlogo.png",
    onlineHelpRefUrl ="index.html?contextID=task_lmc_yjv_sbb",
    resetOffset = true,
    upgrader = WindowsEventLogUpgrader.class,
    upgraderDef = "upgrader/WindowsEventLogDSource.yaml"
)
@ConfigGroups(Groups.class)
public class WindowsEventLogDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "WINDOWS_EVENT_LOG",
      label = "Reader API Type",
      description = "Windows Event Log Reader API Type",
      displayPosition = 40,
      group = "WINDOWS"
  )
  @ValueChooserModel(ReaderAPITypeChooserValues.class)
  public ReaderAPIType readerAPIType = ReaderAPIType.EVENT_LOGGING;

  @ConfigDefBean(groups = "WINDOWS")
  public CommonConfigBean commonConf;

  @ConfigDefBean(groups = "WINDOWS")
  public WinEventLogConfigBean winEventLogConf;

  @Override
  protected Source createSource() {
    return new WindowsEventLogSource(commonConf, winEventLogConf);
  }
}
