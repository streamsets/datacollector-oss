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
package com.streamsets.pipeline.stage.origin.windows;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class CommonConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "Application",
      label = "Windows log to read from",
      description = "Log Name",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(LogNameChooserValues.class)
  public LogName logName = LogName.Application;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Custom Log Name",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "logName",
      triggeredByValue = {"Custom"}
  )
  public String customLogName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ALL",
      label = "Read Mode",
      description = "Read all events in the log or only new events that occur after the pipeline starts",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(ReadModeChooserValues.class)
  public ReadMode readMode = ReadMode.ALL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Buffer Size",
      description = "Buffer Size used to render events. Leave -1 for auto assignment.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      min = -1,
      max = Integer.MAX_VALUE
  )
  public int bufferSize;

}
