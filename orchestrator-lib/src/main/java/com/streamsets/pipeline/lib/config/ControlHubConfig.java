/*
 * Copyright 2021  StreamSets Inc.
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
package com.streamsets.pipeline.lib.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class ControlHubConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "https://cloud.streamsets.com",
      label = "Control Hub URL",
      description = "URL for the Control Hub API",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CONTROL_HUB"
  )
  public String baseUrl = "https://cloud.streamsets.com";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User Name",
      description = "Control Hub User Name",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CONTROL_HUB"
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Control Hub Password",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CONTROL_HUB"
  )
  public CredentialValue password = () -> "";

}
