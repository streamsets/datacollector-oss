/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.hbase.api;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.hbase.api.common.Errors;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public interface HBaseConnectionHelper {

  Logger LOG = LoggerFactory.getLogger(HBaseConnectionHelper.class);

  /**
   * @param issues List of initialization issues
   * @param context Stage context
   * @param hbaseName Connection name
   * @param hbaseConfDir Configuration directory
   * @param tableName Name of the table
   * @param hbaseConfigs Map of configs
   */
  void createHBaseConfiguration(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      String hbaseName,
      String hbaseConfDir,
      String tableName,
      Map<String, String> hbaseConfigs
  );

  /**
   * Fetches the pre-created HBase configuration
   *
   * @return Configuration
   */
  Configuration getHBaseConfiguration();

  /**
   * Validates the security configuration
   *
   * @param issues List of initialization issues
   * @param context Stage context
   * @param hbaseName HBase connection name
   * @param hbaseUser HBase user
   * @param kerberosAuth Kerberos Auth
   */
  void validateSecurityConfigs(
      List<Stage.ConfigIssue> issues, Stage.Context context, String hbaseName, String hbaseUser, boolean kerberosAuth
  );

  /**
   * Sets a INT property in HBase Configuration
   *
   * @param propertyName the name of the property
   * @param value the value of the property
   */
  void setConfigurationIntProperty(String propertyName, Integer value);

  /**
   * Fetch UserGroupInformation
   *
   * @return the UserGroupInformation
   */
  UserGroupInformation getUGI();

  /**
   * Checks if the connection to hbase exists and a given table is created
   *
   * @param issues List of initialization issues
   * @param context stage context
   * @param hbaseName connection name
   * @param tableName table name
   * @return descriptor of the table
   */
  HTableDescriptor checkConnectionAndTableExistence(
      List<Stage.ConfigIssue> issues, Stage.Context context, String hbaseName, String tableName
  );

  /**
   * Fetches the family and qualifier of a given column in string
   *
   * @param column column as string
   * @return HBaseColumn with qualifier and family
   */
  HBaseColumn getColumn(String column);

  static void validateQuorumConfigs(
      List<Stage.ConfigIssue> issues,
      Stage.Context context,
      String hbaseName,
      String zookeeperQuorum,
      String zookeeperParentZNode,
      int clientPort
  ) {
    if (zookeeperQuorum == null || zookeeperQuorum.isEmpty()) {
      issues.add(context.createConfigIssue(hbaseName, "zookeeperQuorum", Errors.HBASE_04));
    } else {
      List<String> zkQuorumList = Lists.newArrayList(Splitter.on(",")
                                                             .trimResults()
                                                             .omitEmptyStrings()
                                                             .split(zookeeperQuorum));
      for (String hostName : zkQuorumList) {
        try {
          InetAddress.getByName(hostName);
        } catch (UnknownHostException ex) {
          LOG.warn(Utils.format("Cannot resolve host: '{}' from zookeeper quorum '{}', error: '{}'",
              hostName,
              zookeeperQuorum,
              ex
          ), ex);
          issues.add(context.createConfigIssue(hbaseName, "zookeeperQuorum", Errors.HBASE_39, hostName));
        }
      }
    }
    if (zookeeperParentZNode == null || zookeeperParentZNode.isEmpty()) {
      issues.add(context.createConfigIssue(hbaseName, "zookeeperBaseDir", Errors.HBASE_09));
    }
    if (clientPort == 0) {
      issues.add(context.createConfigIssue(hbaseName, "clientPort", Errors.HBASE_13));

    }
  }
}
