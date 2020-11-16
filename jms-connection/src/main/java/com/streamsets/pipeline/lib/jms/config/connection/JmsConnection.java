/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.pipeline.lib.jms.config.connection;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.ConnectionEngine;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.credential.CredentialValue;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
@ConnectionDef(
    label = "JMS",
    type = JmsConnection.TYPE,
    description = "Connects to JMS",
    version = 1,
    upgraderDef = "upgrader/JmsConnectionUpgrader.yaml",
    supportedEngines = { ConnectionEngine.COLLECTOR }
)
@ConfigGroups(JmsConnectionGroups.class)
public class JmsConnection {

  public static final String TYPE = "STREAMSETS_JMS";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JMS Initial Context Factory",
      description = "ActiveMQ example: org.apache.activemq.jndi.ActiveMQInitialContextFactory",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JMS"
  )
  public String initialContextFactory;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JNDI Connection Factory",
      description = "ActiveMQ example: ConnectionFactory",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JMS"
  )
  public String connectionFactory;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "JMS Provider URL",
      description = "ActiveMQ example: tcp://mqserver:61616",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "JMS"
  )
  public String providerURL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Credentials",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      defaultValue = "true"
  )
  public boolean useCredentials;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      dependsOn = "useCredentials",
      triggeredByValue = "true"
  )
  public CredentialValue password;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "[]",
      label = "Additional Security Properties",
      description = "Key-value pairs appended to the additional configs in the stage. " +
          "These properties take precedence over those defined in additional stage properties.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS"
  )
  @ListBeanModel
  public List<SecurityPropertyBean> additionalSecurityProps = new ArrayList<>();
}
