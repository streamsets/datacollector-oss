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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttClientConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(MqttClientConfigBean.class);

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Broker URL",
      defaultValue = "tcp://localhost:1883",
      description = "Specify the MQTT Broker URL",
      displayPosition = 10,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MQTT"
  )
  public String brokerUrl = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Client ID",
      defaultValue = "${pipeline:id()}",
      description = "Specify the MQTT Client ID. It must be unique across all clients connecting to the same server.",
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MQTT"
  )
  public String clientId = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Quality of Service",
      defaultValue = "AT_MOST_ONCE",
      description = "Specify the quality of service to publish the message.",
      displayPosition = 40,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MQTT"
  )
  @ValueChooserModel(QualityOfServiceChooserValues.class)
  public QualityOfService qos = QualityOfService.AT_MOST_ONCE;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Client Persistence Mechanism",
      defaultValue = "MEMORY",
      description = "Specify the persistence mechanism used to enable reliable messaging. For messages sent " +
          "at least once (1) or exactly once (2) to be reliably delivered, messages must be stored on both the " +
          "client and server until the delivery of the message is complete.",
      displayPosition = 50,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MQTT"
  )
  @ValueChooserModel(MqttPersistenceMechanismChooserValues.class)
  public MqttPersistenceMechanism persistenceMechanism = MqttPersistenceMechanism.MEMORY;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Client Persistence Data Directory",
      defaultValue = "/tmp",
      description = "Specify the directory for file-based Persistence Mechanism",
      displayPosition = 51,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MQTT",
      dependsOn = "persistenceMechanism",
      triggeredByValue = {"FILE"}
  )
  public String dataDir = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "60",
      label = "Keep Alive Interval (secs)",
      description = "This value defines the maximum time interval between messages sent or received. ",
      displayPosition = 60,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "MQTT"
  )
  public int keepAlive = 60;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Credentials",
      description = "Use Username and Password Authentication",
      defaultValue = "false",
      displayPosition = 70,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MQTT"
  )
  public boolean useAuth = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Clean Session",
      description = "Controls the 'clean session' flag on the MQTT client; refer to MQTT documentation for specifics",
      displayPosition = 100,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "MQTT"
  )
  public boolean cleanSession;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Username",
      displayPosition = 71,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      dependsOn = "useAuth",
      triggeredByValue = { "true" }
  )
  public CredentialValue username;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Password",
      displayPosition = 72,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "CREDENTIALS",
      dependsOn = "useAuth",
      triggeredByValue = { "true" }
  )
  public CredentialValue password;

  @ConfigDefBean(groups = "TLS")
  public TlsConfigBean tlsConfig = new TlsConfigBean();

}
