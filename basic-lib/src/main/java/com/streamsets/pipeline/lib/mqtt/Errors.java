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
package com.streamsets.pipeline.lib.mqtt;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  MQTT_00("MQTT Connection Lost: {}"),
  MQTT_01("Error sending resource. Status: {} Reason: {}"),
  MQTT_02("Error sending resource. Reason: {}"),
  MQTT_03("Error when disconnecting MQTT Client. Reason: {}"),
  MQTT_04("Failed to connect : {}"),
  MQTT_05("Topic cannot be empty"),
  MQTT_06("Invalid topic expression '{}': {}"),
  MQTT_07("Topic White List cannot be empty if topic is resolved at runtime"),
  MQTT_08("Topic expression '{}' generated a null or empty topic for record '{}'"),
  MQTT_09("Topic '{}' resolved from record '{}' is not among the allowed topics"),
  MQTT_10("Error evaluating topic expression '{}' for record '{}': {}"),
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
