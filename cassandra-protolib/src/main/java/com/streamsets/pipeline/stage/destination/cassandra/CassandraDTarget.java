/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "Cassandra",
    description = "Writes data to Cassandra",
    icon = "cassandra.png",
    upgrader = CassandraTargetUpgrader.class,
    onlineHelpRefUrl = "index.html#Destinations/Cassandra.html#task_t1d_z3l_sr"
)
@ConfigGroups(value = Groups.class)
public class CassandraDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "[\"localhost\"]",
      label = "Cassandra Contact Points",
      description = "Hostnames of Cassandra nodes to use as contact points. To ensure a connection, enter several.",
      displayPosition = 10,
      group = "CASSANDRA"
  )
  public List<String> contactNodes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "9042",
      label = "Cassandra Port",
      description = "Port number to use when connecting to Cassandra nodes",
      displayPosition = 20,
      group = "CASSANDRA"
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "LZ4",
      label = "Compression",
      description = "Optional compression for transport-level requests and responses.",
      displayPosition = 30,
      group = "CASSANDRA"
  )
  @ValueChooserModel(CompressionChooserValues.class)
  public CassandraCompressionCodec compression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Credentials",
      defaultValue = "false",
      displayPosition = 40,
      group = "CASSANDRA"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Fully Qualified Table Name",
      description = "Table write to, e.g. <keyspace>.<table_name>",
      displayPosition = 50,
      group = "CASSANDRA"
  )
  public String qualifiedTableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Field to Column Mapping",
      description = "Fields to map to Cassandra columns. To avoid errors, field data types must match.",
      displayPosition = 60,
      group = "CASSANDRA"
  )
  @ListBeanModel
  public List<CassandraFieldMappingConfig> columnNames;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      defaultValue = "",
      displayPosition = 10,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Password",
      defaultValue = "",
      displayPosition = 20,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String password;

  @Override
  protected Target createTarget() {
    return new CassandraTarget(
        contactNodes,
        port,
        compression.getCodec(),
        username,
        password,
        qualifiedTableName,
        columnNames
    );
  }
}
