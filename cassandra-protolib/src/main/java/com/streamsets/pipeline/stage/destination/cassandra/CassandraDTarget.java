/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.cassandra;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Cassandra",
    description = "Writes data to Cassandra",
    icon = "cassandra.png")
@ConfigGroups(value = Groups.class)
public class CassandraDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "[\"localhost\"]",
      label = "Cassandra Contact Point(s)",
      description = "Cassandra nodes to use as contact points. To ensure a connection, enter several.",
      displayPosition = 10,
      group = "CASSANDRA"
  )
  public List<String> contactNodes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "9042",
      label = "Cassandra Port",
      description = "Port to use when connecting to Cassandra",
      displayPosition = 20,
      group = "CASSANDRA"
  )
  public int port;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Credentials",
      defaultValue = "false",
      description = "Enables Credentials tab for authentication connections to C*.",
      displayPosition = 25,
      group = "CASSANDRA"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Username",
      description = "Username",
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
      description = "Cassandra Password",
      defaultValue = "",
      displayPosition = 20,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public String password;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Cassandra Fully Qualified Table Name",
      description = "Fully qualified table name (including key space) to write to. e.g. my_keyspace.my_table",
      displayPosition = 30,
      group = "CASSANDRA"
  )
  public String qualifiedTableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields",
      description = "Selected fields are mapped to columns of the same name. These should match your table schema",
      displayPosition = 40,
      group = "CASSANDRA"
  )
  @ComplexField
  public List<CassandraFieldMappingConfig> columnNames;

  @Override
  protected Target createTarget() {
    return new CassandraTarget(contactNodes, port, username, password, qualifiedTableName, columnNames);
  }
}
