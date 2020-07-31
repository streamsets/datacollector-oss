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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import com.streamsets.pipeline.stage.common.CredentialsConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseRabbitConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "URI",
      defaultValue = "amqp://",
      description = "RabbitMQ URI e.g. amqp://host:port/virtualhost",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "#0"
  )
  public String uri = "amqp://";


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
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "EXCHANGE"
  )
  @ListBeanModel
  public List<RabbitExchangeConfigBean> exchanges = new ArrayList<>();

  @ConfigDefBean(groups = "RABBITMQ")
  public CredentialsConfig credentialsConfig = new CredentialsConfig();

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Additional Client Configuration",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public Map<String, Object> rabbitmqProperties = new HashMap<>();

  /** TLS Configuration Properties */
  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

  /** Advanced Configuration Properties */
  @ConfigDefBean(groups = "ADVANCED")
  public RabbitAdvancedConfigBean advanced = new RabbitAdvancedConfigBean();


  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    for (RabbitExchangeConfigBean exchange : exchanges) {
      exchange.init(context, issues);
    }
  }
}
