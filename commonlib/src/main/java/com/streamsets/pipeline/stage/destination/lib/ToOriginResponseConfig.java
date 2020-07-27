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
package com.streamsets.pipeline.stage.destination.lib;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class ToOriginResponseConfig {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Send Response to Origin",
      displayPosition = 400,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean sendResponseToOrigin = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "SUCCESS_RECORDS",
      label = "Response Type",
      displayPosition = 410,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = "sendResponseToOrigin",
      triggeredByValue = "true"
  )
  @ValueChooserModel(ResponseTypeChooserValues.class)
  public ResponseType responseType = ResponseType.SUCCESS_RECORDS;

}
