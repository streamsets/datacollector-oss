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
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Cassandra",
    description = "Writes data to Cassandra",
    icon = "cassandra.png")
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
  @ValueChooser(CompressionChooserValues.class)
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
  @ComplexField(CassandraFieldMappingConfig.class)
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
