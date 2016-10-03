/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;

public class MysqlSourceConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Hostname",
      description = "MySql server hostname",
      displayPosition = 10,
      group = "MYSQL"
  )
  public String hostname;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "3306",
      label = "Port",
      description = "MySql server port",
      displayPosition = 20,
      group = "MYSQL"
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      description = "MySql username. User must have REPLICATION SLAVE privilege",
      displayPosition = 30,
      group = "CREDENTIALS"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Password",
      description = "MySql user password.",
      displayPosition = 40,
      group = "CREDENTIALS"
  )
  public String password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "999",
      label = "ServerId",
      description = "ServerId used by binlog client. Must be unique among all replication slaves " +
          "(origin acts as a replication slave itself).",
      displayPosition = 50,
      group = "MYSQL"
  )
  public int serverId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max batch size",
      description = "Maximum number of records in a batch.",
      displayPosition = 60,
      group = "ADVANCED"
  )
  public int maxBatchSize;


  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max wait timeout (ms)",
      description = "Maximum timeout millis to wait for batch records before returning " +
          "incomplete or empty batch.",
      displayPosition = 50,
      group = "ADVANCED"
  )
  public int maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Connect timeout (ms)",
      description = "MySql connection timeout millis.",
      displayPosition = 60,
      group = "ADVANCED"
  )
  public int connectTimeout;

  @ConfigDef(
      required = true,
      type = Type.BOOLEAN,
      defaultValue = "false",
      label = "Use SSL",
      description = "Flat to use or not SSL for MySql connection. This is used only for ",
      displayPosition = 65,
      group = "ADVANCED"
  )
  public boolean useSsl;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Start from beginning",
      description = "On first origin start read events from beginning of binlog. " +
          "When 'false' - start from current binlog position. " +
          "In case when GTID-enabled this records all server executed gtids as applied.",
      displayPosition = 70,
      group = "MYSQL"
  )
  public boolean startFromBeginning;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Initial offset",
      description = "On first origin start read events starting from given offset. " +
          "Offset format depends on GTID mode. When GTID is enabled - it should be a GTID-set of " +
          "transactions that should be skipped. When GTID is disabled - it should be binlog filename + binlog " +
          "position to start from in format '${binlog-filename}:${binlog-position}'. " +
          "Note - this setting conflicts with 'Start from beginning' setting, " +
          "if both are set - this takes precedence.",
      displayPosition = 80,
      group = "MYSQL"
  )
  public String initialOffset;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Include tables",
      description = "Comma-delimited list of database and table names to include. " +
          "Database and table names support wildcards - special character '%' match any number of any chars. " +
          "DB and table name are delimited by dot. Example - 'db%sales.sales_%_dep,db2.orders'. " +
          "All tables that are not included are ignored.",
      displayPosition = 90,
      group = "ADVANCED"
  )
  public String includeTables;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Ignore tables",
      description = "Comma-delimited list of database and table names to ignore. " +
          "Database and table names support wildcards - special character '%' match any number of any chars. " +
          "DB and table name are delimited by dot. Example - 'db%sales.sales_%_dep,db2.orders'. " +
          "Ignore tables have precedence over include tables - if some table is both included " +
          "and ignored - it will be ignored.",
      displayPosition = 100,
      group = "ADVANCED"
  )
  public String ignoreTables;

  @Override
  public String toString() {
    return "MysqlSourceConfig{" +
        "hostname='" + hostname + '\'' +
        ", port=" + port +
        ", username='" + username + '\'' +
        ", password='**********'" +
        ", serverId=" + serverId +
        ", maxBatchSize=" + maxBatchSize +
        ", maxWaitTime=" + maxWaitTime +
        ", startFromBeginning=" + startFromBeginning +
        ", offset=" + initialOffset +
        '}';
  }
}
