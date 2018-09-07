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
package com.streamsets.pipeline.hbase.api.common;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  HBASE_00("Hadoop UserGroupInformation reports Simple authentication, it should be Kerberos"),
  HBASE_01("Failed to configure or connect to the '{}' HBase: {}"),
  HBASE_02("Error while writing batch of records to HBase"),
  HBASE_04("Zookeeper quorum cannot be empty"),
  HBASE_05("Table name cannot be empty "),
  HBASE_06("Cannot connect to cluster: {}"),
  HBASE_07("Table name doesn't exist: {}"),
  HBASE_08("Table is not enabled: {}"),
  HBASE_09("Zookeeper root znode cannot be empty "),
  HBASE_10("Failed writing record: {}"),
  HBASE_11("Cannot parse family and qualifier for record: {} and row key: {}"),
  HBASE_12("Cannot convert type: {} to {}"),
  HBASE_13("Zookeeper client port is invalid: {}"),
  HBASE_14("Invalid row key storage type: {}"),
  HBASE_15("Invalid column storage type: {}"),
  HBASE_16("Hadoop UserGroupInformation should return Kerberos authentication, it is set to: {}"),
  HBASE_17("Failed to configure or connect to the HBase cluster: {}"),
  HBASE_18("HBase column mapping should be defined or implicit field mapping should be enabled"),
  HBASE_19("HBase configuration directory '{}' under SDC resources does not exist"),
  HBASE_20("HBase configuration directory '{}' path under  SDC resources is not a directory"),
  HBASE_21("HBase configuration file '{}/{}' under SDC resources is not a file"),
  HBASE_22("Could not resolve the default Kerberos realm, you must set the 'hbase.master.kerberos.principal' " +
    "property to the HBase master principal name: {}"),
  HBASE_23("Could not resolve the default Kerberos realm, you must set the 'hbase.regionserver.kerberos.principal' " +
    "property to the HBase RegionServer principal name: {}"),
  HBASE_24("HBase Configuration Directory '{}' must be relative to SDC resources directory in cluster mode"),
  HBASE_25("Missing column field '{}' in record"),
  HBASE_26("Error while writing to HBase: '{}'"),
  HBASE_27("Missing row key field '{}' in record"),
  HBASE_28("Cannot construct HBase column from '{}' as it is not separated by '{}'"),
  HBASE_29("Record root type: '{}' is not eligible to be inserted in HBase; Root type must be a MAP or LIST_MAP "),
  HBASE_30("All fields encountered error: '{}'"),
  HBASE_31("Error converting '{}' to '{}'"),
  HBASE_32("Column family '{}' doesn't exist for table '{}'"),
  HBASE_33("Invalid time driver expression"),
  HBASE_34("Could not evaluate time driver expression: {}"),
  HBASE_35("Row key field has empty value"),
  HBASE_36("Error while reading from HBase: '{}'"),
  HBASE_37("Error while reading invalid column from HBase: '{}'"),
  HBASE_38("Could not evaluate expression: '{}'"),
  HBASE_39("Cannot resolve host '{}' from ZooKeeper quorum"),
  HBASE_40("Output field has empty value"),
  HBASE_41("No key on Record '{}' with key:'{}', column:'{}', timestamp:'{}'"),
  HBASE_42("Error while opening HBase table: '{}'"),
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
