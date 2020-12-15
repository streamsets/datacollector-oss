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

package com.streamsets.pipeline.lib.connection.snowflake;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

@ConnectionDef(
    label = "Snowflake",
    type = SnowflakeConnection.TYPE,
    description = "Connects to Snowflake",
    version = 1,
    upgraderDef = "upgrader/SnowflakeConnectionUpgrader.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR, ConnectionEngine.TRANSFORMER }
)
@ConfigGroups(SnowflakeConnectionGroups.class)
public class SnowflakeConnection {

  public static final String TYPE = "STREAMSETS_SNOWFLAKE";

  @ConfigDef(
      required = false,
      defaultValue = "US_WEST_2",
      type = ConfigDef.Type.MODEL,
      label = "Snowflake Region",
      displayPosition = 10,
      group = "#0"
  )
  @ValueChooserModel(SnowflakeRegionChooserValues.class)
  public SnowflakeRegion snowflakeRegion;

  @ConfigDef(
      required = true,
      defaultValue = "",
      type = ConfigDef.Type.STRING,
      label = "Custom Snowflake Region",
      description = "Typical formats are \"us-east-1\" or \"east-us-2.azure\"",
      displayPosition = 20,
      group = "#0",
      dependencies = @Dependency(configName = "snowflakeRegion", triggeredByValues = "OTHER")
  )
  @ValueChooserModel(SnowflakeRegionChooserValues.class)
  public String customSnowflakeRegion;

  @ConfigDef(
      required = true,
      defaultValue = "jdbc:snowflake:[HOST]:[PORT]",
      type = ConfigDef.Type.CREDENTIAL,
      label = "Virtual Private Snowflake URL",
      displayPosition = 30,
      group = "#0",
      dependencies = @Dependency(configName = "snowflakeRegion", triggeredByValues = "CUSTOM_URL")
  )
  public CredentialValue customUrl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Account",
      description = "The Snowflake account is the first part of the account URL, for example: <account>.us-east-1" +
          ".snowflakecomputing.com",
      displayPosition = 40,
      group = "#0"
  )
  public CredentialValue account;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "User",
      displayPosition = 50,
      group = "#0"
  )
  public CredentialValue user;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 55,
      group = "#0"
  )
  public CredentialValue password;

}
