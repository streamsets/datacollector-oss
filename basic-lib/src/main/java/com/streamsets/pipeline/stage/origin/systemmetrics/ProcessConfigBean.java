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
package com.streamsets.pipeline.stage.origin.systemmetrics;

import com.streamsets.pipeline.api.ConfigDef;

public class ProcessConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Processes",
      description = "Regexp to filter the processes",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROCESS",
      defaultValue = ".*",
      dependsOn = "fetchProcessStats^",
      triggeredByValue = "true"
  )
  public String processRegexStr = ".*";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "User",
      description = "User regexp to filter the processes",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "PROCESS",
      defaultValue = ".*",
      dependsOn = "fetchProcessStats^",
      triggeredByValue = "true"
  )
  public String userRegexStr = ".*";

}
