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
 * limitations under the License.```
 */
package com.streamsets.pipeline.stage.destination.tooriginresponse;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

@StageDef(
    version = 2,
    label = "Send Response to Origin",
    description = "Sends error records and the specified status code to a response-enabled origin",
    icon="response.png",
    upgraderDef = "upgrader/ToErrorOriginResponseDTarget.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_mzv_rgp_q2b"
)
@ErrorStage
@HideStage(HideStage.Type.ERROR_STAGE)
@GenerateResourceBundle
public class ToErrorOriginResponseDTarget  extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Status Code",
      defaultValue = "500",
      description = "Response Status Code",
      displayPosition = 10
  )
  public int statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Response Headers",
      description = "Headers to include in the response",
      displayPosition = 20
  )
  public Map<String, String> headers = new HashMap<>();

  @Override
  protected Target createTarget() {
    return new ToOriginResponseTarget(statusCode, headers);
  }

}
