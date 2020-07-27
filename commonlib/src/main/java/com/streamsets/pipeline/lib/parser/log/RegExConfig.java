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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.ConfigDef;

public class RegExConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue="/",
    label = "Field Path",
    description = "",
    displayPosition = 10,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public String fieldPath;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue="1",
    label = "Regular Expression Group",
    description="",
    displayPosition = 20,
    displayMode = ConfigDef.DisplayMode.BASIC
  )
  public int group;

}
