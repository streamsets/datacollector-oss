/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  HADOOPFS_14("Cannot write record '{}': {}"),
  HADOOPFS_15("Record in error: {}"),
  HADOOPFS_16("Unsupported data format '{}'"),
  HADOOPFS_17("Cannot initialize the late records writer manager: {}"),
  HADOOPFS_18("URI '{}' must start with <scheme>://<path>"),
  HADOOPFS_19("Invalid time basis expression '{}': {}"),
  HADOOPFS_20("Invalid directory template: {}"),
  HADOOPFS_21("Invalid late record directory template: {}"),
  HADOOPFS_22("Invalid URI '{}': {}"),
  HADOOPFS_23("Could not commit old files: {}"),

  HADOOPFS_24("Could nto evaluate EL for directory path: {}"),

  HADOOPFS_25("Hadoop configuration directory '{}' under SDC resources does not exist"),
  HADOOPFS_26("Hadoop configuration directory '{}' path under  SDC resources is not a directory"),
  HADOOPFS_27("Hadoop configuration file '{}/{}' under SDC resources is not a file"),

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
