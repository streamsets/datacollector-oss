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
package com.streamsets.pipeline.stage.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;

public class MysqlBinLogSourceConfig extends MySQLHikariPoolConfigBean {

  public static final String CONFIG_PREFIX = "config.";
  public static final String CONFIG_HOSTNAME = CONFIG_PREFIX + "hostname";
  public static final String CONFIG_INITIAL_OFFSET = CONFIG_PREFIX + "initialOffset";
  public static final String CONFIG_INCLUDE_TABLES = CONFIG_PREFIX + "includeTables";
  public static final String CONFIG_IGNORE_TABLES = CONFIG_PREFIX + "ignoreTables";

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Hostname",
      description = "MySql server hostname",
      displayPosition = 10,
      group = "JDBC"
  )
  public String hostname;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "3306",
      label = "Port",
      description = "MySql server port",
      displayPosition = 20,
      group = "JDBC"
  )
  public int port;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      description = "MySql username. User must have REPLICATION SLAVE privilege.",
      displayPosition = 30,
      group = "CREDENTIALS"
  )
  public CredentialValue username;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      description = "MySql user password",
      displayPosition = 40,
      group = "CREDENTIALS"
  )
  public CredentialValue password;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "",
      label = "Server ID",
      description = "Replication server ID that the origin uses to connect to the master MySQL server. " +
          "Must be unique from the server ID of the replication master and of all other replication slaves.",
      displayPosition = 50,
      group = "JDBC"
  )
  public int serverId;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (records)",
      description = "Maximum number of records in a batch",
      displayPosition = 60,
      group = "ADVANCED"
  )
  public int maxBatchSize;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Wait Time (ms)",
      description = "Maximum number of milliseconds to wait before sending a partial or empty batch",
      displayPosition = 50,
      group = "ADVANCED"
  )
  public int maxWaitTime;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Connect Timeout (ms)",
      description = "Maximum time in milliseconds to wait for a connection to the MySQL server",
      displayPosition = 60,
      group = "ADVANCED"
  )
  public int connectionTimeout = 5000;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enable KeepAlive Thread",
      description = "Uses a Keep Alive thread to maintain the connection to the MySQL server",
      displayPosition = 65,
      group = "ADVANCED"
  )
  public boolean enableKeepAlive;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60000",
      label = "KeepAlive Interval (ms)",
      description = "Maximum number of milliseconds to keep an idle Keep Alive thread active before closing the thread",
      dependsOn = "enableKeepAlive",
      triggeredByValue = "true",
      displayPosition = 70,
      group = "ADVANCED"
  )
  public long keepAliveInterval;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Start From Beginning",
      description = "Starts reading events from the beginning of the binary log. " +
          "When not selected, the origin begins reading events from the last saved offset",
      displayPosition = 70,
      group = "JDBC"
  )
  public boolean startFromBeginning;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.BASIC,
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Initial offset",
      description = "Read events starting at this offset in the binary log. The offset format depends on the GTID mode.",
      displayPosition = 80,
      group = "JDBC",
      dependsOn = "startFromBeginning",
      triggeredByValue = "false"
  )
  public String initialOffset;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Include Tables",
      description = "List of tables to include when reading change events in the binary log file",
      displayPosition = 90,
      group = "ADVANCED"
  )
  public String includeTables;

  @ConfigDef(
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Ignore Tables",
      description = "List of tables to ignore when reading change events in the binary log file",
      displayPosition = 100,
      group = "ADVANCED"
  )
  public String ignoreTables;


  public String buildConnectionString() {
    readOnly = true;
    return String.format("%s%s:%d",
        MySQLHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX,
        hostname,
        port
    );
  }

  public BasicConnectionString getBasicConnectionString() {
    return new BasicConnectionString(getPatterns(), getConnectionStringTemplate());
  }

  @Override
  public String toString() {
    return "MysqlBinLogSourceConfig{" +
        "hostname='" + hostname + '\'' +
        ", port=" + port +
        ", username=''**********'" +
        ", password='**********'" +
        ", serverId=" + serverId +
        ", maxBatchSize=" + maxBatchSize +
        ", maxWaitTime=" + maxWaitTime +
        ", startFromBeginning=" + startFromBeginning +
        ", offset=" + initialOffset +
        '}';
  }
}
