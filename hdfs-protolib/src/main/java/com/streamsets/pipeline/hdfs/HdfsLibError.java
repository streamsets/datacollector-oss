/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.ErrorCode;

public enum HdfsLibError implements ErrorCode {
  HDFS_0001("HDFS Security is not enabled in the Hadoop configuration, the HDFS destination is configured with Kerberos"),
  HDFS_0002("HDFS Security is enabled in the Hadoop configuration, the HDFS destination is configured without Kerberos"),
  HDFS_0003("Failed to connect to HDFS FileSystem URI='{}', {}"),
  HDFS_0004("Invalid path template '{}', {}"),
  HDFS_0005("Invalid late-path template '{}', {}"),
  HDFS_0006("The specified custom compression codec '{}' is not a compression codec"),
  HDFS_0007("The specified custom compression codec '{}' could not be loaded, {}"),
  HDFS_0008("The specified late records time limit expression is invalid '{}', {}"),
  HDFS_0009("The specified time driver expression is invalid '{}', {}"),
  HDFS_0010("The specified time driver expression does not return a date '{}'"),
  HDFS_0011("The maximum file size value must be zero greater than zero, '{}'"),
  HDFS_0012("The maximum records per file must be zero greater than zero, '{}'"),
  HDFS_0013("The late record time limit expression '{}' must be greater than zero"),

  ;
  private final String msg;

  HdfsLibError(String msg) {
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
