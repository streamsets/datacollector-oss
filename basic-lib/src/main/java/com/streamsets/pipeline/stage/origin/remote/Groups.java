/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
  SFTP("SFTP"),
  CREDENTIALS("Credentials"),
  ERROR("Error Handling"),
  PROXY("Proxy"),
  TEXT("Text"),
  JSON("JSON"),
  DELIMITED("Delimited"),
  XML("XML"),
  LOG("Log"),
  AVRO("Avro"),
  BINARY("Binary"),
  PROTOBUF("Protobuf"),
  ;

  private final String label;

  private Groups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return this.label;
  }
}
