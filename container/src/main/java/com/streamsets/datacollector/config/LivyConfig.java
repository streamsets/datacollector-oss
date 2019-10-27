/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.credential.CredentialValue;

public class LivyConfig {

  @ConfigDef(
      type = ConfigDef.Type.STRING,
      required = true,
      label = "Livy Endpoint",
      description = "URL to connect to the Livy Server",
      defaultValue = "https://localhost:30443/gateway/default/livy/v1/",
      group = "CLUSTER",
      displayPosition = 105,
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"SQL_SERVER_BIG_DATA_CLUSTER", "AZURE_HD_INSIGHT"}
          )
      }
  )
  public String baseUrl = "https://localhost:30443/gateway/default/livy/v1/";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "Livy user name",
      displayPosition = 107,
      group = "CLUSTER",
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"SQL_SERVER_BIG_DATA_CLUSTER", "AZURE_HD_INSIGHT"}
          )
      }
  )
  public CredentialValue username = () -> "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "Password for the account",
      displayPosition = 108,
      group = "CLUSTER",
      dependencies = {
          @Dependency(
              configName = "^clusterConfig.clusterType",
              triggeredByValues = {"SQL_SERVER_BIG_DATA_CLUSTER", "AZURE_HD_INSIGHT"}
          )
      }
  )
  public CredentialValue password = () -> "";

}
