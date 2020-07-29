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
package com.streamsets.pipeline.hbase.api.common.producer;

import com.streamsets.pipeline.api.ConfigDef;

import java.util.Map;

public class HBaseConnectionConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "ZooKeeper Quorum",
      description = "Comma separated list of servers in the ZooKeeper Quorum. " +
          "For example, host1.mydomain.com,host2.mydomain.com,host3.mydomain.com ",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public String zookeeperQuorum;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2181",
      label = "ZooKeeper Client Port",
      description = "The ZooKeeper port at which clients connect",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public int clientPort;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/hbase",
      label = "ZooKeeper Parent Znode",
      description = "Root Znode for HBase in ZooKeeper",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public String zookeeperParentZNode;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Table Name",
      description = "The qualified table name. Use format <NAMESPACE>.<TABLE>. " +
          "If namespace is not specified, namespace 'default' will be assumed",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public String tableName;

  @ConfigDef(required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Kerberos Authentication",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public boolean kerberosAuth;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HBase User",
      description = "If set, the data collector will write to HBase as this user. " +
          "The data collector user must be configured as a proxy user in HDFS.",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public String hbaseUser;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "HBase Configuration Directory",
      description = "An absolute path or a directory under SDC resources directory to load hbase-site.xml configuration file",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public String hbaseConfDir;

  @ConfigDef(required = false,
      type = ConfigDef.Type.MAP,
      label = "HBase Configuration",
      description = "Additional HBase client properties",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "HBASE"
  )
  public Map<String, String> hbaseConfigs;
}
