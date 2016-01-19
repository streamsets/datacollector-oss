/**
 * Copyright 2015 StreamSets Inc.
 * <p/>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  RABBITMQ_01("Failed to connect to RabbitMQ: '{}'"),
  RABBITMQ_02("Failed to acknowledge offset '{}': {}"),
  RABBITMQ_03("Invalid URI provided: {}"),
  RABBITMQ_04("Failed to parse record from received message: '{}'"),
  RABBITMQ_05("Connection to RabbitMQ has been lost."),
  RABBITMQ_06("Exchange name cannot be empty. To bind only to the default (nameless) exchange, remove all bindings."),
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

