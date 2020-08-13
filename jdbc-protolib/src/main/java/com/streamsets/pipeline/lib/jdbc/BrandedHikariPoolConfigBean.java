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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;

public class BrandedHikariPoolConfigBean extends HikariPoolConfigBean {

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JDBC Connection String",
      displayPosition = 10,
      group = "JDBC"
  )
  public String connectionString = "";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Use Credentials",
      displayPosition = 15,
      group = "JDBC"
  )
  public boolean useCredentials = true;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Username",
      displayPosition = 110,
      group = "CREDENTIALS"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      dependsOn = "useCredentials",
      triggeredByValue = "true",
      label = "Password",
      displayPosition = 120,
      group = "CREDENTIALS"
  )
  public CredentialValue password;


  @Override
  public String getConnectionString() {
    return connectionString;
  }

  @Override
  public DatabaseVendor getVendor() {
    return DatabaseVendor.forUrl(connectionString);
  }

  @Override
  public CredentialValue getUsername() {
    return username;
  }

  @Override
  public CredentialValue getPassword() {
    return password;
  }

  @Override
  public boolean useCredentials() {
    return useCredentials;
  }
}
