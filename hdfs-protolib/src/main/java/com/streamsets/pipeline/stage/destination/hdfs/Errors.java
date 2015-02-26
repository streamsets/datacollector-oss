/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ErrorCode;

public enum Errors implements ErrorCode {
  HADOOPFS_00("Hadoop UserGroupInformation reports SIMPLE authentication, it should be KERBEROS"),
  HADOOPFS_01("Failed to configure/connect to the '{}' Hadoop File System, {}"),
  HADOOPFS_02("Invalid dir path template '{}', {}"),

  HADOOPFS_03("The specified Key expression is invalid '{}', {}"),
  HADOOPFS_04("The specified custom compression codec '{}' is not a compression codec"),
  HADOOPFS_05("The specified custom compression codec '{}' could not be loaded, {}"),
  HADOOPFS_06("The specified late records time limit expression is invalid '{}', {}"),
  HADOOPFS_07("The specified time driver expression is invalid '{}', {}"),
  HADOOPFS_08("The maximum file size value must be zero or greater than zero"),
  HADOOPFS_09("The maximum records per file must be zero or greater than zero"),
  HADOOPFS_10("The late record time limit expression must be greater than zero"),

  HADOOPFS_11("Could not initialize the writer manager: {}"),

  HADOOPFS_12("The record '{}' is late"),
  HADOOPFS_13("Error while writing to HDFS: {}"),
  HADOOPFS_14("Could not write record '{}', {}"),
  HADOOPFS_15("Record in error: {}"),
  HADOOPFS_16("Unsupported data format '{}'"),
  HADOOPFS_17("Could not initialize the late records writer manager: {}"),
  HADOOPFS_18("URI '{}' must start with '<scheme>://'"),
  HADOOPFS_19("Invalid time basis expression '{}', {}"),
  HADOOPFS_20("Invalid directory path template, {}"),
  HADOOPFS_21("Invalid late directory path template, {}"),
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
