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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  ELASTICSEARCH_00("Could not parse the index template expression: {}"),
  ELASTICSEARCH_01("Could not evaluate the index template expression: {}"),
  ELASTICSEARCH_02("Could not parse the type template expression: {}"),
  ELASTICSEARCH_03("Could not evaluate the type template expression: {}"),
  ELASTICSEARCH_04("Could not parse the docId template expression: {}"),
  ELASTICSEARCH_05("Could not evaluate the docId template expression: {}"),
  ELASTICSEARCH_06("Cluster name cannot be empty"),
  ELASTICSEARCH_07("At least one URI must be provided"),
  ELASTICSEARCH_08("Could not connect to the cluster: {}"),
  ELASTICSEARCH_09("Invalid URI, it must be <HOSTNAME>:<PORT>: '{}'"),
  ELASTICSEARCH_10("Port value out of range: '{}'"),

  ELASTICSEARCH_15("Could not write record '{}': {}"),
  ELASTICSEARCH_16("Could not index record '{}': {}"),
  ELASTICSEARCH_17("Could not index '{}' records: {}"),

  ELASTICSEARCH_18("Could not evaluate the time driver expression: {}"),
  ELASTICSEARCH_19("Document ID expression must be provided to use the upsert option"),

  ELASTICSEARCH_20("Invalid Shield user, it must be <USERNAME>:<PASSWORD>: '{}'"),
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
