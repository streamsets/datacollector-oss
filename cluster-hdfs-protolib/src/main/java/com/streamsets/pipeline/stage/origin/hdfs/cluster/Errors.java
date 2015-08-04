/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
  HADOOPFS_13("Authority of URI cannot be null"),
  HADOOPFS_14("Invalid URI authority '{}', authority should only consist of host and port"),
  HADOOPFS_15("HDFS location is not a directory: '{}'"),
  HADOOPFS_16("Cannot generate splits, directory '{}'" + " might not have any files" ),
  HADOOPFS_17("Cannot validate kerberos configuration: {}"),
  HADOOPFS_18("No directories specified"),
  HADOOPFS_22("Invalid URI '{}': {}"),
  HADOOPFS_25("Hadoop configuration directory '{}' does not exist"),
  HADOOPFS_26("Hadoop configuration directory '{}' is not a directory"),
  HADOOPFS_27("Hadoop configuration file '{}'  is not a file"),
  HADOOPFS_28("Could not resolve the default Kerberos realm, you must set the 'dfs.namenode.kerberos.principal' " +
    "property to the HDFS principal name: {}"),


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
