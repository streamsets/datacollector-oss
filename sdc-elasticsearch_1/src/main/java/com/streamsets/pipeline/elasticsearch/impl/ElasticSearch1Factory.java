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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugins.PluginsService;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public class ElasticSearch1Factory extends ElasticSearchFactory {

  @Override
  public Client createClient(
      String clusterName,
      List<String> uris,
      boolean clientSniff,
      Map<String, String> configs,
      boolean useShield,
      String shieldUser,
      boolean shieldTransportSsl,
      String sslKeystorePath,
      String sslKeystorePassword,
      boolean useFound
  ) throws UnknownHostException {
    ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder()
        .put("client.transport.sniff", clientSniff)
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

    Settings settings = settingsBuilder.build();
    InetSocketTransportAddress[] elasticAddresses = new InetSocketTransportAddress[uris.size()];
    for (int i = 0; i < uris.size(); i++) {
      String uri = uris.get(i);
      String[] parts = uri.split(":");
      elasticAddresses[i] = new InetSocketTransportAddress(parts[0], Integer.parseInt(parts[1]));
    }
    return new TransportClient(settings, false).addTransportAddresses(elasticAddresses);
  }

  @Override
  public Settings createSettings(Map<String, Object> configs) {
    ImmutableSettings.Builder settings = ImmutableSettings.builder();
    for (Map.Entry<String, Object> config : configs.entrySet()) {
      settings.put(config.getKey(), config.getValue());
    }
    // Needed to avoid "shield plugin requires the license plugin to be installed" error in unit tests
    settings.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false);
    return settings.build();
  }

}
