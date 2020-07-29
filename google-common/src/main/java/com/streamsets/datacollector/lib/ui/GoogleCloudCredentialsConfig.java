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

package com.streamsets.datacollector.lib.ui;

import com.streamsets.pipeline.api.ConfigDef;

public class GoogleCloudCredentialsConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Credentials File Path (JSON)",
      description = "Absolute path or path relative to Data Collector resources directory",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS"
  )
  public String path = "";
}
