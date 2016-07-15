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
package com.streamsets.pipeline.lib.rabbitmq.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.config.DataFormat;

@GenerateResourceBundle
public enum Groups implements Label {
  RABBITMQ("RabbitMQ"),
  CREDENTIALS("Credentials"),
  QUEUE("Queue"),
  EXCHANGE("Exchange"),
  ADVANCED("Advanced"),
  TEXT(DataFormat.TEXT.getLabel()),
  JSON(DataFormat.JSON.getLabel()),
  DELIMITED(DataFormat.DELIMITED.getLabel()),
  XML(DataFormat.XML.getLabel()),
  LOG(DataFormat.LOG.getLabel()),
  AVRO(DataFormat.AVRO.getLabel()),
  BINARY(DataFormat.BINARY.getLabel()),
  PROTOBUF(DataFormat.PROTOBUF.getLabel()),
  DATAGRAM(DataFormat.DATAGRAM.getLabel()),
  WHOLE_FILE(DataFormat.WHOLE_FILE.getLabel()),
  ;

  private final String label;

  Groups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return this.label;
  }
}
