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
package com.streamsets.pipeline.stage.destination.mqtt;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.destination.http.DataFormatChooserValues;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

public class MqttClientTargetConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Runtime Topic Resolution",
      description = "Select topic at runtime based on the field values in the record",
      displayPosition = 21,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public boolean runtimeTopicResolution;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${record:value('/topic')}",
      label = "Topic Expression",
      description = "An expression that resolves to the name of the topic to use",
      displayPosition = 22,
      displayMode = ConfigDef.DisplayMode.BASIC,
      elDefs = {RecordEL.class},
      group = "MQTT",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      dependsOn = "runtimeTopicResolution",
      triggeredByValue = "true"
  )
  public String topicExpression;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      lines = 5,
      defaultValue = "*",
      label = "Topic White List",
      description = "A comma-separated list of valid topic names. " +
          "Records with invalid topic names are treated as error records. " +
          "'*' indicates that all topic names are allowed.",
      displayPosition = 23,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MQTT",
      dependsOn = "runtimeTopicResolution",
      triggeredByValue = "true"
  )
  public String topicWhiteList;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Topic",
      defaultValue = "",
      description = "Specify the topic to deliver the message to, for example \"finance/stock/cmp\"",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MQTT",
      dependsOn = "runtimeTopicResolution",
      triggeredByValue = "false"
  )
  public String topic = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Retain the Message",
      description = "Whether or not the publish message should be retained by the messaging engine",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MQTT"
  )
  public boolean retained = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "MQTT payload data format",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

}
