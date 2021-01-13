/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.stage.connection.mysqlbinlog;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.credential.CredentialValue;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "MySQL Binary Log",
    type = MySQLBinLogConnection.TYPE,
    description = "MySQL Binary Log connector",
    version = 1,
    upgraderDef = "upgrader/MySQLBinLogConnection.yaml",
    supportedEngines = {ConnectionEngine.COLLECTOR}
)
@ConfigGroups(MySQLBinLogConnectionGroups.class)
public class MySQLBinLogConnection {
  public static final String TYPE = "STREAMSETS_MYSQL_BIN_LOG";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Hostname",
      description = "MySql server hostname",
      displayPosition = 10,
      group = "#0"
  )
  public String hostname;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "3306",
      label = "Port",
      description = "MySql server port",
      displayPosition = 20,
      group = "#0"
  )
  public String port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "MySql username. User must have REPLICATION SLAVE privilege",
      displayPosition = 30,
      group = "#1"
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "MySql user password.",
      displayPosition = 40,
      group = "#1"
  )
  public CredentialValue password;

  @ConfigDef(
      required = true,
      type = Type.BOOLEAN,
      defaultValue = "false",
      label = "Use SSL",
      description = "Whether to use SSL for the MySQL connection",
      displayPosition = 80,
      group = "#2"
  )
  public boolean useSsl;

  @Override
  public String toString() {
    return "MysqlSourceConfig{" +
        "hostname='" + hostname + '\'' +
        ", port=" + port +
        ", username='" + username + '\'' +
        ", password='**********'" +
        ", useSSL=" + useSsl +
        '}';
  }
}
