/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.common.emr;

import com.streamsets.pipeline.api.ConfigDef;

public class BootstrapAction {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Location",
      description = "S3 URI where the action script is located",
      displayPosition = 2121,
      defaultValue = ""
  )
  public String location;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      description = "Arguments of the action script",
      label = "Arguments",
      displayPosition = 2122,
      defaultValue = ""
  )
  public String arguments;
}
