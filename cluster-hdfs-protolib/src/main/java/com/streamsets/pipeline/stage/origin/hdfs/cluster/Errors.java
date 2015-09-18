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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  HADOOPFS_00("HDFS URI must be specified"),
  HADOOPFS_02("HDFS URI must start with <scheme>://<path>"),
  HADOOPFS_03("Invalid hdfs location '{}': {}"),
  HADOOPFS_04("Max json object length cannot be less than 1"),
  HADOOPFS_05("Max text object length cannot be less than 1 "),
  HADOOPFS_06("Unsupported data format '{}'"),
  HADOOPFS_08("Cannot parse record '{}': {}"),
  HADOOPFS_09("Cannot obtain splits for '{}': {}"),
  HADOOPFS_10("HDFS location doesn't exist: '{}'"),
  HADOOPFS_11("Cannot connect to the filesytem. Check if the hdfs location: '{}' is valid or not: '{}'"),
  HADOOPFS_12("Invalid scheme '{}', scheme should be hdfs" ),
  HADOOPFS_19("HDFS URI is not set and is also not available through 'fs.defaultFS' config"),
  HADOOPFS_13("Authority of URI cannot be null"),
  HADOOPFS_15("HDFS location is not a directory: '{}'"),
  HADOOPFS_16("Cannot generate splits, directory '{}' might not have any files" ),
  HADOOPFS_17("Cannot validate kerberos configuration: {}"),
  HADOOPFS_18("No directories specified"),
  HADOOPFS_22("Invalid URI '{}': {}"),
  HADOOPFS_25("Hadoop configuration directory '{}' does not exist"),
  HADOOPFS_26("Hadoop configuration directory '{}' is not a directory"),
  HADOOPFS_27("Hadoop configuration file '{}'  is not a file"),
  HADOOPFS_28("Could not resolve the default Kerberos realm, you must set the 'dfs.namenode.kerberos.principal' " +
    "property to the HDFS principal name: {}"),
  HADOOPFS_29("Hadoop configuration directory '{}' must be relative to SDC resources directory in cluster mode"),
  HADOOPFS_30("Max data object length cannot be less than 1")
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
