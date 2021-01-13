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
package com.streamsets.pipeline.stage.origin.mysql;

import com.streamsets.pipeline.api.ConfigDef;

public class MySQLBinLogConfig {
  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "999",
      label = "Server ID",
      description = "ServerId used by binlog client. Must be unique among all replication slaves " +
          "(origin acts as a replication slave itself).",
      displayPosition = 50,
      group = "#0"
  )
  public String serverId;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (records)",
      description = "Maximum number of records in a batch.",
      displayPosition = 60,
      group = "#2"
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Wait Time (ms)",
      description = "Maximum timeout millis to wait for batch records before returning " +
          "incomplete or empty batch.",
      displayPosition = 50,
      group = "#2"
  )
  public int maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Connect Timeout (ms)",
      description = "MySql connection timeout millis.",
      displayPosition = 60,
      group = "#2"
  )
  public long connectTimeout;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Enable KeepAlive Thread",
      description = "Whether keepAlive thread should be automatically started",
      displayPosition = 65,
      group = "#2"
  )
  public boolean enableKeepAlive;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60000",
      label = "KeepAlive Interval (ms)",
      description = "Keep Alive interval in milliseconds. 1 minute by default",
      dependsOn = "enableKeepAlive",
      triggeredByValue = "true",
      displayPosition = 70,
      group = "#2"
  )
  public long keepAliveInterval;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Start From Beginning",
      description = "On first origin start read events from beginning of binlog. " +
          "When 'false' - start from current binlog position. " +
          "In case when GTID-enabled this records all server executed gtids as applied.",
      displayPosition = 70,
      group = "#0"
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
      group = "#0"
  )
  public String initialOffset;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Include Tables",
      description = "Comma-delimited list of database and table names to include. " +
          "Database and table names support wildcards - special character '%' match any number of any chars. " +
          "DB and table name are delimited by dot. Example - 'db%sales.sales_%_dep,db2.orders'. " +
          "All tables that are not included are ignored.",
      displayPosition = 90,
      group = "#2"
  )
  public String includeTables;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Ignore Tables",
      description = "Comma-delimited list of database and table names to ignore. " +
          "Database and table names support wildcards - special character '%' match any number of any chars. " +
          "DB and table name are delimited by dot. Example - 'db%sales.sales_%_dep,db2.orders'. " +
          "Ignore tables have precedence over include tables - if some table is both included " +
          "and ignored - it will be ignored.",
      displayPosition = 100,
      group = "#2"
  )
  public String ignoreTables;

}
