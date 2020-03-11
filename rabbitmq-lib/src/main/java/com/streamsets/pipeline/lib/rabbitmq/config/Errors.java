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
package com.streamsets.pipeline.lib.rabbitmq.config;

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
  RABBITMQ_07("Failed to write record to RabbitMQ: {}"),
  RABBITMQ_08("RabbitMQ error - Records are returned, check Mandatory flag." +
      " ReplyCode - {}, ReplyText - {}, Exchange - {}, RoutingKey - {}"),
  RABBITMQ_09("Invalid Configuration Value - {} for {}"),
  RABBITMQ_10("Queue name can not be empty. The RabbitMQ Consumer needs a specific queue to read from."),
  RABBITMQ_11("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
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
