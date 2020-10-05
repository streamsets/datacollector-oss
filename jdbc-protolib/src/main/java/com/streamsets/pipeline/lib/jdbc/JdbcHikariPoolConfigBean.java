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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.jdbc.connection.JdbcConnection;
import com.streamsets.pipeline.lib.jdbc.multithread.DatabaseVendor;

public class JdbcHikariPoolConfigBean extends HikariPoolConfigBean {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      connectionType = JdbcConnection.TYPE,
      defaultValue = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL,
      label = "Connection",
      group = "#0",
      displayPosition = -500
  )
  @ValueChooserModel(ConnectionDef.Constants.ConnectionChooserValues.class)
  public String connectionSelection = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL;

  @ConfigDefBean(
      groups = {"JDBC", "CREDENTIALS"},
      dependencies = {
          @Dependency(
              configName = "connectionSelection",
              triggeredByValues = ConnectionDef.Constants.CONNECTION_SELECT_MANUAL
          )
      }
  )
  public JdbcConnection connection;


  @Override
  public String getConnectionString() {
    return connection.connectionString;
  }

  @Override
  public DatabaseVendor getVendor() {
    return DatabaseVendor.forUrl(connection.connectionString);
  }

  @Override
  public CredentialValue getUsername() {
    return connection.username;
  }

  @Override
  public CredentialValue getPassword() {
    return connection.password;
  }

  @Override
  public boolean useCredentials() {
    return connection.useCredentials;
  }
}
