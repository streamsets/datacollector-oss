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
package com.streamsets.pipeline.stage.destination.maprdb;

import com.google.protobuf.ServiceException;
import com.streamsets.pipeline.stage.destination.hbase.HBaseFieldMappingConfig;
import com.streamsets.pipeline.stage.destination.hbase.HBaseTarget;
import com.streamsets.pipeline.stage.destination.hbase.StorageType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Overridden HBaseTarget that disables checks that are not relevant to Mapr DB (zookeeper and such).
 */
public class MapRDBTarget extends HBaseTarget {

  public MapRDBTarget(
        String zookeeperQuorum,
        int clientPort,
        String zookeeperParentZnode,
        String tableName,
        String hbaseRowKey,
        StorageType rowKeyStorageType,
        List<HBaseFieldMappingConfig> hbaseFieldColumnMapping,
        boolean kerberosAuth,
        String hbaseConfDir,
        Map<String, String> hbaseConfigs,
        String hbaseUser,
        boolean implicitFieldMapping,
        boolean ignoreMissingFieldPath,
        boolean ignoreInvalidColumn,
        String timeDriver
  ) {
    super(
      zookeeperQuorum,
      clientPort,
      zookeeperParentZnode,
      tableName,
      hbaseRowKey,
      rowKeyStorageType,
      hbaseFieldColumnMapping,
      kerberosAuth,
      hbaseConfDir,
      hbaseConfigs,
      hbaseUser,
      implicitFieldMapping,
      ignoreMissingFieldPath,
      ignoreInvalidColumn,
      timeDriver
    );
  }

  @Override
  protected void validateQuorumConfigs(List<ConfigIssue> issues) {
    // Disable zooekeepr checks because zookeeper is not needed and we're hiding it's configuration completely
  }

  @Override
  protected void checkHBaseAvailable(Configuration conf) throws IOException, ServiceException {
    // Call to HBaseAdmin.checkHBaseAvailable is not supported on MapR DB
    // http://doc.mapr.com/display/MapR/Support+for+the+HBaseAdmin+class
  }
}
