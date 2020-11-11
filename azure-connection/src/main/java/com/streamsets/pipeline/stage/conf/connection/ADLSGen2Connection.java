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
package com.streamsets.pipeline.stage.conf.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "Azure Data Lake Storage Gen2",
    type = ADLSGen2Connection.TYPE,
    description = "Connects to Azure Data Lake Storage Gen2",
    version = 1,
    upgraderDef = "upgrader/ADLSGen2Connection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER}
)
@ConfigGroups(AzureConnectionGroups.class)
public class ADLSGen2Connection extends AzureConnection {

  public static final String TYPE = "STREAMSETS_ADLS_GEN2";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Secure Connection",
      description = "Enable a secure connection using abfss",
      displayPosition = 3,
      group = "#0"
  )
  public boolean secureConnection;
}
