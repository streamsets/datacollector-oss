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
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.DataFormatChooserValues;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.CredentialsConfig;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "URI",
      defaultValue = "amqp://",
      description = "RabbitMQ URI e.g. amqp://host:port/virtualhost",
      displayPosition = 10,
      group = "#0"
  )
  public String uri = "amqp://";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Consumer Tag",
      defaultValue = "",
      description = "Leave blank to use an auto-generated tag.",
      displayPosition = 20,
      group = "#0"
  )
  public String consumerTag = "";

  /** Exchange Configuration Properties */
  @ConfigDefBean(groups = "QUEUE")
  public RabbitQueueConfigBean queue = new RabbitQueueConfigBean();

  /** Exchange Configuration Properties */
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Bindings",
      description = "Optional list of exchange bindings.",
      displayPosition = 40,
      group = "EXCHANGE"
  )
  @ListBeanModel
  public List<RabbitExchangeConfigBean> exchanges = new ArrayList<>();

  @ConfigDefBean(groups = "RABBITMQ")
  public BasicConfig basicConfig = new BasicConfig();

  @ConfigDefBean(groups = "RABBITMQ")
  public CredentialsConfig credentialsConfig = new CredentialsConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      displayPosition = 3000,
      group = "#0"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = "RABBITMQ")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Additional Client Configuration",
      displayPosition = 40,
      group = "#0"
  )
  public Map<String, Object> rabbitmqProperties = new HashMap<>();

  /** Advanced Configuration Properties */
  @ConfigDefBean(groups = "ADVANCED")
  public RabbitAdvancedConfigBean advanced = new RabbitAdvancedConfigBean();

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    for (RabbitExchangeConfigBean exchange : exchanges) {
      exchange.init(context, issues);
    }
  }
}
