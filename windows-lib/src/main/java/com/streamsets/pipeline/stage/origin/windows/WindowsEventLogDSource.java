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
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DSource;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Windows Event Log",
    description = "Reads data from a Windows event log",
    execution = {ExecutionMode.EDGE},
    icon = "winlogo.png",
    onlineHelpRefUrl = "index.html#Origins/WindowsLog.html#task_lmc_yjv_sbb"
)

@ConfigGroups(Groups.class)
public class WindowsEventLogDSource extends DSource {

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "Application",
      label = "Windows log to read from",
      description = "Log Name",
      displayPosition = 10,
      group = "WINDOWS"
  )
  @ValueChooserModel(LogNameChooserValues.class)
  public LogName logName = LogName.Application;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ALL",
      label = "Read Mode",
      description = "Read all events in the log or only new events that occur after the pipeline starts",
      displayPosition = 20,
      group = "WINDOWS"
  )
  @ValueChooserModel(ReadModeChooserValues.class)
  public ReadMode readMode = ReadMode.ALL;

  @Override
  protected Source createSource() {
    return new WindowsEventLogSource(logName, readMode);
  }
}
