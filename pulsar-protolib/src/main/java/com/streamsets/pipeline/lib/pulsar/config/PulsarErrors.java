/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.pulsar.config;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum PulsarErrors implements ErrorCode {
  // Configuration errors
  PULSAR_00("Could not create Pulsar client with service URL '{}': {}"),
  PULSAR_01("Could not write record: {}"),
  PULSAR_02("Cloud not evaluate record expression: {}"),
  PULSAR_03("Error when sending message to destination '{}': {}"),
  PULSAR_04("Cloud not send message to destination '{}': {}"),
  PULSAR_05("Could not create Pulsar consumer for topic pattern '{}' and subscription '{}': {}"),
  PULSAR_06("Could not create Pulsar consumer with topics list for subscription '{}': {}"),
  PULSAR_07("Error when acknowledging message with message ID '{}' : {}"),
  PULSAR_08("Error when consuming messages: {}"),
  PULSAR_09("Cannot parse record from message '{}': {}"),
  PULSAR_10("Could not create Pulsar consumer with destination topic '{}' and subscription '{}': {}"),
  PULSAR_11("Certification Authority certificate file path cannot be null nor empty"),
  PULSAR_12("Certification Authority certificate file not found at specified location. Relative path is '{}' and full" +
      " path is '{}'"),
  PULSAR_13("Client certificate file path cannot be null nor empty"),
  PULSAR_14("Client certificate file not found at specified location. Relative path is '{}' and full path is '{}'"),
  PULSAR_15("Client key file path cannot be null nor empty"),
  PULSAR_16("Client key file not found at specified location. Relative path is '{}' and full path is '{}'"),
  PULSAR_17("Specified authentication is not supported: {}"),
  PULSAR_18("Batch size greater than maximal batch size allowed in sdc.properties, maxBatchSize: {}"),
  ;


  private final String msg;

  PulsarErrors(String msg) {
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
