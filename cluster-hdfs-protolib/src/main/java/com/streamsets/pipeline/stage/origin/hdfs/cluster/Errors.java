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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  HADOOPFS_00("Hadoop UserGroupInformation reports '{}' authentication, it should be '{}'"),
  HADOOPFS_02("Hadoop FS URI must start with <scheme>://<path>"),
  HADOOPFS_08("Cannot parse record '{}': {}"),
  HADOOPFS_09("Cannot obtain splits for '{}': {}"),
  HADOOPFS_10("Hadoop FS location doesn't exist: '{}'"),
  HADOOPFS_11("Cannot connect to the filesystem. Check if the Hadoop FS location: '{}' is valid or not: '{}'"),
  HADOOPFS_19("Hadoop FS URI is not set and is also not available through the cluster configs"),
  HADOOPFS_13("Authority of URI cannot be null"),
  HADOOPFS_15("Hadoop FS location is not a directory: '{}'"),
  HADOOPFS_16("Error reading file '{}' due to '{}'" ),
  HADOOPFS_17("Directory '{}' doesn't have any files"),
  HADOOPFS_18("No directories specified"),
  HADOOPFS_22("Invalid URI '{}': {}"),
  HADOOPFS_25("Hadoop configuration directory '{}' does not exist"),
  HADOOPFS_26("Hadoop configuration directory '{}' is not a directory"),
  HADOOPFS_27("Hadoop configuration file '{}'  is not a file"),
  HADOOPFS_28("Could not resolve the default Kerberos realm, you must set the 'dfs.namenode.kerberos.principal' " +
    "property to the Hadoop FS principal name: {}"),
  HADOOPFS_29("Hadoop configuration directory '{}' (resolved to '{}') is not inside SDC resources directory '{}'."),
  HADOOPFS_30("Hadoop configuration file '{}' does not exist"),
  HADOOPFS_31("Hadoop configuration dir with config files core-site.xml, hdfs-site.xml, yarn-site.xml and " +
      "mapred-site.xml is required"),
  HADOOPFS_32("Lines to Skip is not applicable for Cluster Batch Mode and should be set to 0"),

  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
