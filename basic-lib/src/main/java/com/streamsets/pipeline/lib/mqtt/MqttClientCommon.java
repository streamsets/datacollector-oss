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

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class MqttClientCommon {
  private static final String SSL_CONFIG_PREFIX = "commonConf.tlsConfig.";
  private final MqttClientConfigBean commonConf;
  private MqttClient mqttClient;

  public MqttClientCommon(MqttClientConfigBean commonConf) {
    this.commonConf = commonConf;
  }

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    if (commonConf.tlsConfig.isEnabled()) {
      // this configuration has no separate "tlsEnabled" field on the bean level, so need to do it this way
      commonConf.tlsConfig.init(
          context,
          Groups.TLS.name(),
          SSL_CONFIG_PREFIX,
          issues
      );
    }
  }

  public MqttClient createMqttClient(MqttCallback mqttCallback) throws MqttException, StageException {
    MqttClientPersistence clientPersistence;
    if (commonConf.persistenceMechanism == MqttPersistenceMechanism.MEMORY) {
      clientPersistence = new MemoryPersistence();
    } else {
      clientPersistence = new MqttDefaultFilePersistence(commonConf.dataDir);
    }
    mqttClient = new MqttClient(commonConf.brokerUrl, commonConf.clientId, clientPersistence);
    mqttClient.setCallback(mqttCallback);
    MqttConnectOptions connOpts = new MqttConnectOptions();
    connOpts.setCleanSession(commonConf.cleanSession);
    connOpts.setKeepAliveInterval(commonConf.keepAlive);
    if (commonConf.useAuth) {
      connOpts.setUserName(commonConf.username.get());
      connOpts.setPassword(commonConf.password.get().toCharArray());
    }
    configureSslContext(commonConf.tlsConfig, connOpts);
    mqttClient.connect(connOpts);
    return mqttClient;
  }


  private void configureSslContext(TlsConfigBean conf, MqttConnectOptions connOpts) {
    try {
      URI vURI = new URI(commonConf.brokerUrl);
      if (vURI.getScheme().equals("ssl")) {
        SSLContext sslContext = conf.getSslContext();
        if (sslContext != null) {
          connOpts.setSocketFactory(sslContext.getSocketFactory());
        }
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(commonConf.brokerUrl);
    }
  }

  public void destroy() {

  }
}
