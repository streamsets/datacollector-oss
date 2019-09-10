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
package com.streamsets.pipeline.solr.api;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  SOLR_00("Solr URI cannot be empty"),
  SOLR_01("ZooKeeper Connection String cannot be empty"),
  SOLR_02("Fields value cannot be empty"),
  SOLR_03("Could not connect to the Solr instance: {}"),
  SOLR_04("Could not write record '{}': {}"),
  SOLR_05("Could not index '{}' records: {}"),
  SOLR_06("Record is missing mapped fields: {}"),
  SOLR_07("Record is missing required fields: {}"),
  SOLR_08("Record is missing optional fields: {}"),
  SOLR_09("Record Field type in '{}' is not correct. It must be MAP or LIST_MAP but it is '{}'"),
  SOLR_10("Record does not contain any value in path: {}"),
  SOLR_11("Fields path cannot be empty"),
  SOLR_12("Mapping is missing Solr required fields: {}"),
  SOLR_13("Mapping is missing Solr optional fields: {}"),
  SOLR_14("Connection timeout cannot be negative"),
  SOLR_15("Socket timeout cannot be negative")
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
