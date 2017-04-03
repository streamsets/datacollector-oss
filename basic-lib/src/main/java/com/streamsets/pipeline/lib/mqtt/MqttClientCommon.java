/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.pipeline.lib.mqtt;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.SslConfigBean;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.glassfish.jersey.SslConfigurator;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class MqttClientCommon {
  private static final String SSL_CONFIG_PREFIX = "conf.sslConfig.";
  private final MqttClientConfigBean commonConf;
  private MqttClient mqttClient;

  public MqttClientCommon(MqttClientConfigBean commonConf) {
    this.commonConf = commonConf;
  }

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    commonConf.sslConfig.init(
        context,
        Groups.SSL.name(),
        SSL_CONFIG_PREFIX,
        issues
    );
  }

  public MqttClient createMqttClient(MqttCallback mqttCallback) throws MqttException {
    MqttClientPersistence clientPersistence;
    if (commonConf.persistenceMechanism == MqttPersistenceMechanism.MEMORY) {
      clientPersistence = new MemoryPersistence();
    } else {
      clientPersistence = new MqttDefaultFilePersistence(commonConf.dataDir);
    }
    mqttClient = new MqttClient(commonConf.brokerUrl, commonConf.clientId, clientPersistence);
    mqttClient.setCallback(mqttCallback);
    MqttConnectOptions connOpts = new MqttConnectOptions();
    connOpts.setCleanSession(false);
    connOpts.setKeepAliveInterval(commonConf.keepAlive);
    if (commonConf.useAuth) {
      connOpts.setUserName(commonConf.username);
      connOpts.setPassword(commonConf.password.toCharArray());
    }
    configureSslContext(commonConf.sslConfig, connOpts);
    mqttClient.connect(connOpts);
    return mqttClient;
  }


  private void configureSslContext(SslConfigBean conf, MqttConnectOptions connOpts) {
    try {
      URI vURI = new URI(commonConf.brokerUrl);
      if (vURI.getScheme().equals("ssl")) {
        SslConfigurator sslConfig = SslConfigurator.newInstance();
        if (!StringUtils.isEmpty(conf.trustStorePath) && !StringUtils.isEmpty(conf.trustStorePassword)) {
          sslConfig.trustStoreFile(conf.trustStorePath).trustStorePassword(conf.trustStorePassword);
        }

        if (!StringUtils.isEmpty(conf.keyStorePath) && !StringUtils.isEmpty(conf.keyStorePassword)) {
          sslConfig.keyStoreFile(conf.keyStorePath).keyStorePassword(conf.keyStorePassword);
        }

        SSLContext sslContext = sslConfig.createSSLContext();
        connOpts.setSocketFactory(sslContext.getSocketFactory());
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(commonConf.brokerUrl);
    }
  }

  public void destroy() {

  }
}
