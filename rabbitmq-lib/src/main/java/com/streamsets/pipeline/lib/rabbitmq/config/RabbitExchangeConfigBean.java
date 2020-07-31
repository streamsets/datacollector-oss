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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitExchangeConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Name",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String name = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Type",
      defaultValue = "DIRECT",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  @ValueChooserModel(ExchangeTypeChooserValues.class)
  public ExchangeType type = ExchangeType.DIRECT;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Durable",
      defaultValue = "true",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public boolean durable = true;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Auto-delete",
      defaultValue = "false",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public boolean autoDelete = false;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Routing Key",
      description = "Leave this blank to default to the Queue Name.",
      defaultValue = "",
      dependsOn = "type",
      triggeredByValue = {"DIRECT", "TOPIC"},
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String routingKey = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Declaration Properties",
      description = "Additional exchange declaration configuration.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public Map<String, Object> declarationProperties = new HashMap<>();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Binding Properties",
      description = "Additional exchange binding configuration.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public Map<String, Object> bindingProperties = new HashMap<>();

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (name.isEmpty()) {
      issues.add(context.createConfigIssue(Groups.EXCHANGE.name(), "exchanges.name", Errors.RABBITMQ_06));
    }
  }
}
