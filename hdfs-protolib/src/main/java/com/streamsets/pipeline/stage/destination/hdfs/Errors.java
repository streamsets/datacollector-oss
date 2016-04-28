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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HADOOPFS_00("Hadoop UserGroupInformation reports '{}' authentication, it should be '{}'"),
  HADOOPFS_01("Failed to configure or connect to the '{}' Hadoop file system: {}"),
  HADOOPFS_02("Invalid dir path template '{}', {}"),

  HADOOPFS_03("The sequence file key expression '{}' is invalid: {}"),
    /* LC - okay to add "valid" below? */
  HADOOPFS_04("The custom compression codec '{}' is not a valid compression codec"),
  HADOOPFS_05("The custom compression codec '{}' cannot be loaded: {}"),
  HADOOPFS_06("The late record time limit expression '{}' is invalid: {}"),
  HADOOPFS_07("The time driver expression '{}' is invalid: {}"),
  HADOOPFS_08("The maximum file size must be a positive integer or zero to opt out of the option"),
  HADOOPFS_09("The maximum records in a file must be a positive integer or zero to opt out of the option"),
  HADOOPFS_10("The late record time limit expression must be greater than zero"),

  HADOOPFS_11("Cannot initialize the writer manager: {}"),

  HADOOPFS_12("The record '{}' is late"),
  HADOOPFS_13("Error while writing to HDFS: {}"),
  HADOOPFS_14("Cannot write record: {}"),
  HADOOPFS_15("Record in error: {}"),
  HADOOPFS_16("Unsupported data format '{}'"),
  HADOOPFS_17("Cannot initialize the late records writer manager: {}"),
  HADOOPFS_18("URI '{}' must start with <scheme>://<path>"),
  HADOOPFS_19("Invalid time basis expression '{}': {}"),
  HADOOPFS_20("Invalid directory template: {}"),
  HADOOPFS_21("Invalid late record directory template: {}"),
  HADOOPFS_22("Invalid URI '{}': {}"),
  HADOOPFS_23("Could not commit old files: {}"),

  HADOOPFS_24("Could not evaluate EL for directory path: {}"),

  HADOOPFS_25("Hadoop configuration directory '{}' does not exist"),
  HADOOPFS_26("Hadoop configuration directory '{}' is not a directory"),
  HADOOPFS_27("Hadoop configuration file '{}' is not a file"),
  HADOOPFS_28("Could not resolve the default Kerberos realm, you must set the 'dfs.namenode.kerberos.principal' " +
              "property to the HDFS principal name: {}"),

  HADOOPFS_29("Path template uses the '{}' function, it must use the '{}' function"),
  HADOOPFS_30("The 'every(<UNIT>, <VALUE>)' function can be used only once in the path"),
  HADOOPFS_31("The 'every(<UNIT>, <VALUE>)' function must use YYYY(), YY(), MM(), DD(), hh(), mm() or ss() as <UNIT>"),
  HADOOPFS_32("The 'every(<UNIT>, <VALUE>)' function has the <VALUE> argument out of range, it must be between '1' and '{}'"),
  HADOOPFS_33("The 'every(<UNIT>, <VALUE>)' function must use the smallest unit in the path template"),
  HADOOPFS_34("The 'every(<UNIT>, <VALUE>)' function value must be a sub-multiple of the maximum value of the <UNIT>"),
  HADOOPFS_35("Failed to retrieve increment time unit and value from the path template: {}"),
  HADOOPFS_36("The 'ss()' function cannot be used within and outside of the 'every()' function at the same time"),
  HADOOPFS_37("The 'mm()' function cannot be used within and outside of the 'every()' function at the same time"),

  HADOOPFS_40("Base directory path must be absolute"),
  HADOOPFS_41("Base directory path could not be created"),
  HADOOPFS_42("Base directory path could not be created: '{}'"),
  HADOOPFS_43("Could not create a file/directory under base directory: '{}'"),
  HADOOPFS_44("Could not verify the base directory: '{}'"),
  HADOOPFS_45("Hadoop configuration directory '{}' must be relative to SDC resources directory in cluster mode"),

  HADOOPFS_46("The compression codec '{}' requires native libraries to be installed: {}"),
  HADOOPFS_47("Time basis expression '{}' evaluated to NULL for this record"),
  HADOOPFS_48("Failed to instantiate compression codec due to error: {}"),

  HADOOPFS_49("HDFS URI is not set and is also not available through 'fs.defaultFS' config"),

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
