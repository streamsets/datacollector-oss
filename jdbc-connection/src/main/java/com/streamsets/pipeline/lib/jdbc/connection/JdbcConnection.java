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

package com.streamsets.pipeline.lib.jdbc.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.ConnectionVerifier;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.credential.CredentialValue;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "JDBC",
    type = JdbcConnection.TYPE,
    description = "Connects to JDBC",
    version = 1,
    upgraderDef = "upgrader/JdbcConnectionUpgrader.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER }
)
@ConfigGroups(JdbcConnectionGroups.class)
public class JdbcConnection {

  public static final String TYPE = "STREAMSETS_JDBC";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JDBC Connection String",
      displayPosition = 10,
      group = "#0"
  )
  public String connectionString = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Credentials",
      displayPosition = 15,
      group = "#0"
  )
  public boolean useCredentials = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Username",
      displayPosition = 20,
      group = "#1"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Password",
      displayPosition = 25,
      group = "#1"
  )
  public CredentialValue password;
}
