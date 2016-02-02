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
package com.streamsets.pipeline.elasticsearch.impl;

import com.streamsets.pipeline.elasticsearch.api.ElasticSearchFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.shield.ShieldPlugin;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public class ElasticSearch2Factory extends ElasticSearchFactory {

  @Override
  public Client createClient(
      String clusterName,
      List<String> uris,
      Map<String, String> configs,
      boolean useShield,
      String shieldUser,
      boolean shieldTransportSsl,
      String sslKeystorePath,
      String sslKeystorePassword,
      boolean useFound
  ) throws UnknownHostException {
    Settings.Builder settingsBuilder = Settings.settingsBuilder()
        .put("cluster.name", clusterName)
        .put(configs);

    if (useShield) {
      settingsBuilder = settingsBuilder
          .put("shield.user", shieldUser)
          .put("shield.transport.ssl", shieldTransportSsl);
      if (sslKeystorePath != null && !sslKeystorePath.isEmpty()) {
        settingsBuilder = settingsBuilder.put("shield.ssl.keystore.path", sslKeystorePath);
      }
      if (sslKeystorePassword != null && !sslKeystorePassword.isEmpty()) {
        settingsBuilder = settingsBuilder.put("shield.ssl.keystore.password", sslKeystorePassword);
      }
    }
    if (useFound) {
      settingsBuilder = settingsBuilder
          .put("action.bulk.compress", false) // To use Found, action.bulk.compress must be disabled
          .put("request.headers.X-Found-Cluster", clusterName);
    }

    Settings settings = settingsBuilder.build();
    InetSocketTransportAddress[] elasticAddresses = new InetSocketTransportAddress[uris.size()];
    for (int i = 0; i < uris.size(); i++) {
      String uri = uris.get(i);
      String[] parts = uri.split(":");
      elasticAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1]));
    }
    TransportClient.Builder clientBuilder = TransportClient.builder().settings(settings);
    if (useShield) {
      clientBuilder = clientBuilder.addPlugin(ShieldPlugin.class);
    }
    return clientBuilder.build().addTransportAddresses(elasticAddresses);
  }

  @Override
  public Settings createSettings(Map<String, Object> configs) {
    Settings.Builder settings = Settings.builder();
    for (Map.Entry<String, Object> config : configs.entrySet()) {
      settings.put(config.getKey(), config.getValue());
    }
    return settings.build();
  }

}
