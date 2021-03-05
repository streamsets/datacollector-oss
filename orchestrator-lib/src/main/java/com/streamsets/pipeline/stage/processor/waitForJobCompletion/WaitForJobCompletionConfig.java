/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.waitForJobCompletion;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.config.ControlHubConfig;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;

public class WaitForJobCompletionConfig {


  @ConfigDefBean(groups = {"JOB", "CREDENTIALS", "HTTP", "PROXY", "TLS", "LOGGING"})
  public ControlHubConfig controlHubConfig = new ControlHubConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Status Check Interval",
      description = "Milliseconds to wait between job status checks",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "JOB",
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int waitTime;

}
