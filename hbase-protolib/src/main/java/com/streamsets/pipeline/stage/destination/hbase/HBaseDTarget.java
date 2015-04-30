/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in
 * whole or part without written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import java.util.List;
import java.util.Map;

import com.streamsets.pipeline.api.ComplexField;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.configurablestage.DTarget;

@GenerateResourceBundle
@StageDef(version = "1.0.0", label = "Hbase Target", icon = "hbase.png")
@ConfigGroups(Groups.class)
public class HBaseDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Zookeeper quorum",
      description = "Comma separated list of servers in the ZooKeeper Quorum."
          + "For example, host1.mydomain.com,host2.mydomain.com,host3.mydomain.com ",
      displayPosition = 10,
      group = "HBASE")
  public String zookeeperQuorum;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2181",
      label = "Zookeeper client port",
      description = "The zookeeper port at which clients connect ",
      displayPosition = 20,
      group = "HBASE")
  public int clientPort;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/hbase",
      label = "Zookeeper parent znode",
      description = "Root znode for HBase in ZooKeeper.",
      displayPosition = 30,
      group = "HBASE")
  public String zookeeperParentZnode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Table Name",
      description = "Enter the fully qualified table name. For e.g namespace1.table1. If namespace is not specified,"
          + "namespace 'default' will be assumed",
      displayPosition = 40,
      group = "HBASE")
  public String tableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Row key",
      description = "Field path row key",
      displayPosition = 50,
      group = "HBASE")
  public String hbaseRowKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage type",
      description = "The storage type for row key. It can be either Text or Binary.",
      displayPosition = 60,
      group = "HBASE")
  @ValueChooser(RowKeyStorageTypeChooserValues.class)
  public StorageType rowKeyStorageType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields",
      description = "Column names, their values and storage type",
      displayPosition = 70,
      group = "HBASE")
  @ComplexField
  public List<HBaseFieldMappingConfig> hbaseFieldColumnMapping;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Kerberos Authentication",
      description = "",
      displayPosition = 80,
      group = "HBASE")
  public boolean kerberosAuth;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Kerberos principal",
      description = "",
      displayPosition = 90,
      group = "HBASE",
      dependsOn = "kerberosAuth",
      triggeredByValue = "true")
  public String kerberosPrincipal;

  @ConfigDef(required = false,
      type = ConfigDef.Type.STRING,
      label = "Kerberos keytab",
      description = "Keytab file path",
      displayPosition = 100, group = "HBASE",
      dependsOn = "kerberosAuth",
      triggeredByValue = "true")
  public String kerberosKeytab;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MAP,
      label = "HBase Configuration",
      description = "Additional Hbase client properties",
      displayPosition = 110,
      group = "HBASE")
  public Map<String, String> hbaseConfigs;

  @Override
  protected Target createTarget() {
    return new HBaseTarget(zookeeperQuorum, clientPort, zookeeperParentZnode, tableName, hbaseRowKey,
        rowKeyStorageType, hbaseFieldColumnMapping, kerberosAuth, kerberosPrincipal,
        kerberosKeytab, hbaseConfigs);
  }

}
