/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hbase;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HBASE_00("Hadoop UserGroupInformation reports Simple authentication, it should be Kerberos"),
  HBASE_01("Failed to configure or connect to the '{}' Hbase: {}"),
  HBASE_02("Error while writing batch of records to hbase"),
  HBASE_04("Zookeeper quorum cannot be empty"),
  HBASE_05("Table name cannot be empty "),
  HBASE_06("Cannot connect to cluster: {}"),
  HBASE_07("Table name doesn't exist: {}"),
  HBASE_08("Table is not enabled: {}"),
  HBASE_09("Zookeeper root znode cannot be empty "),
  HBASE_10("Failed writing record '{}': {}"),
  HBASE_11("Cannot parse family and qualifier for record: {} and rowkey: {}"),
  HBASE_12("Cannot convert type: {} to {} for record: {} "),
  HBASE_13("Zookeeper client port is invalid: {}"),
  HBASE_14("Invalid row key storage type: {}"),
  HBASE_15("Invalid column storage type: {}"),
  HBASE_16("Hadoop UserGroupInformation should return kerberos authentication, it is set to: {}"),
  HBASE_17("Failed to configure or connect to the HBase cluster: {}"),
  HBASE_18("HBase column mapping is undefined. There should be atleast one column"),
  HBASE_19("HBase configuration directory '{}' under SDC resources does not exist"),
  HBASE_20("HBase configuration directory '{}' path under  SDC resources is not a directory"),
  HBASE_21("HBase configuration file '{}/{}' under SDC resources is not a file"),
  HBASE_22("Cannot find the value of HBase Master principal (hbase.master.kerberos.principal)"),
  HBASE_23("Cannot find the value of HBase RegionServer principal (hbase.regionserver.kerberos.principal)"),
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
