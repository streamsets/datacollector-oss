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
package com.streamsets.pipeline.stage.destination.rabbitmq;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.Map;

public class BasicPropertiesConfig {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Set AMQP Message Properties",
      description = "Set AMQP Message Properties",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public boolean setAMQPMessageProperties = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Content-Type",
      description = "Content Type",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String contentType;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Content-Encoding",
      description = "Content Encoding",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String contentEncoding;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Headers",
      description = "Headers",
      displayPosition = 80,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public Map<String, Object> headers;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NON_PERSISTENT",
      label = "Delivery Mode",
      description = "DeliveryMode",
      displayPosition = 90,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  @ValueChooserModel(DeliveryModeChooserValues.class)
  public DeliveryMode deliveryMode = DeliveryMode.NON_PERSISTENT;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ZERO",
      label = "Priority",
      description = "Priority",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  @ValueChooserModel(PriorityChooserValues.class)
  public Priority priority = Priority.ZERO;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Correlation Id",
      description = "Correlation Id",
      displayPosition = 110,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String correlationId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Reply To",
      description = "Reply To",
      displayPosition = 120,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String replyTo;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Set Expiration",
      description = "Sets the expiration time in message properties",
      displayPosition = 130,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public boolean setExpiration = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "0",
      label = "Expiration",
      description = "Expiration Time",
      displayPosition = 135,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setExpiration",
      triggeredByValue = "true",
      group = "#0"
  )
  public short expiration = 0;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Message Id",
      description = "Message Id",
      displayPosition = 140,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String messageId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "true",
      label = "Set Current Time",
      description = "Set Current Time Stamp",
      displayPosition = 150,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public boolean setCurrentTime = true;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "",
      label = "Time Stamp",
      description = "Time Stamp",
      displayPosition = 160,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0",
      dependsOn = "setCurrentTime",
      triggeredByValue = "false"
  )
  public long timestamp;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Message Type",
      description = "Message Type",
      displayPosition = 170,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String type;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "User Id",
      description = "Optional user ID. Verified by RabbitMQ against the actual connection username.",
      displayPosition = 180,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String userId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "App Id",
      description = "Identifier of the application that produced the message.",
      displayPosition = 190,
      displayMode = ConfigDef.DisplayMode.BASIC,
      dependsOn = "setAMQPMessageProperties",
      triggeredByValue = "true",
      group = "#0"
  )
  public String appId;

  //Cluster Id is deprecated. No need to add it.

}
