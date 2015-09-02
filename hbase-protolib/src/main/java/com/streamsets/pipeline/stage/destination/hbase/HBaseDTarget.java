/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

package com.streamsets.pipeline.stage.destination.hbase;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DTarget;

import java.util.List;
import java.util.Map;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "HBase",
    description = "Writes data to HBase",
    icon = "hbase.png",
    privateClassLoader = true
)
@ConfigGroups(Groups.class)
public class HBaseDTarget extends DTarget {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "ZooKeeper Quorum",
      description = "Comma separated list of servers in the ZooKeeper Quorum. " +
        "For example, host1.mydomain.com,host2.mydomain.com,host3.mydomain.com ",
      displayPosition = 10,
      group = "HBASE")
  public String zookeeperQuorum;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2181",
      label = "ZooKeeper Client Port",
      description = "The ZooKeeper port at which clients connect",
      displayPosition = 20,
      group = "HBASE")
  public int clientPort;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/hbase",
      label = "ZooKeeper Parent Znode",
      description = "Root Znode for HBase in ZooKeeper",
      displayPosition = 30,
      group = "HBASE")
  public String zookeeperParentZnode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Table Name",
      description = "The qualified table name. Use format <NAMESPACE>.<TABLE>. " +
        "If namespace is not specified, namespace 'default' will be assumed",
      displayPosition = 40,
      group = "HBASE")
  public String tableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Row Key",
      description = "Field path row key",
      displayPosition = 50,
      group = "HBASE")
  public String hbaseRowKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "TEXT",
      label = "Storage Type",
      description = "The storage type for row key",
      displayPosition = 60,
      group = "HBASE")
  @ValueChooserModel(RowKeyStorageTypeChooserValues.class)
  public StorageType rowKeyStorageType;

  @ConfigDef(required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "",
      label = "Fields",
      description = "Column names, their values and storage type",
      displayPosition = 70,
      group = "HBASE")
  @ListBeanModel
  public List<HBaseFieldMappingConfig> hbaseFieldColumnMapping;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Kerberos Authentication",
      displayPosition = 80,
      group = "HBASE")
  public boolean kerberosAuth;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "HBase User",
    description = "If set, the data collector will write to HBase as this user. " +
                  "The data collector user must be configured as a proxy user in HDFS.",
    displayPosition = 90,
    group = "HBASE"
  )
  public String hbaseUser;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "",
    label = "HBase Configuration Directory",
    description = "An absolute path or a directory under SDC resources directory to load hbase-site.xml configuration file",
    displayPosition = 100,
    group = "HBASE")
  public String hbaseConfDir;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MAP,
      label = "HBase Configuration",
      description = "Additional HBase client properties",
      displayPosition = 110,
      group = "HBASE")
  public Map<String, String> hbaseConfigs;

  @Override
  protected Target createTarget() {
    return new HBaseTarget(zookeeperQuorum, clientPort, zookeeperParentZnode, tableName, hbaseRowKey,
        rowKeyStorageType, hbaseFieldColumnMapping, kerberosAuth, hbaseConfDir, hbaseConfigs, hbaseUser);

  }

}
