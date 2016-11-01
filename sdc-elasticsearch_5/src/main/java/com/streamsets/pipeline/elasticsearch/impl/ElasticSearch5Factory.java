/**
 * Copyright 2016 StreamSets Inc.
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
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ElasticSearch5Factory extends ElasticSearchFactory {

  @Override
  public Client createClient(
      String clusterName,
      List<String> uris,
      boolean clientSniff,
      Map<String, String> configs,
      boolean useSecurity,
      String securityUser,
      boolean securityTransportSsl,
      String sslKeystorePath,
      String sslKeystorePassword,
      String sslTruststorePath,
      String sslTruststorePassword,
      boolean useElasticCloud
  ) throws UnknownHostException {
    Settings.Builder settingsBuilder = Settings.builder()
        .put("client.transport.sniff", clientSniff)
        .put("cluster.name", clusterName)
        .put(configs);

    if (useSecurity) {
      settingsBuilder = settingsBuilder
          .put("xpack.security.user", securityUser)
          .put("xpack.security.transport.ssl.enabled", securityTransportSsl);
      if (sslKeystorePath != null && !sslKeystorePath.isEmpty()) {
        settingsBuilder = settingsBuilder.put("xpack.ssl.keystore.path", sslKeystorePath);
      }
      if (sslKeystorePassword != null && !sslKeystorePassword.isEmpty()) {
        settingsBuilder = settingsBuilder.put("xpack.ssl.keystore.password", sslKeystorePassword);
      }
      if (sslTruststorePath != null && !sslTruststorePath.isEmpty()) {
        settingsBuilder = settingsBuilder.put("xpack.ssl.truststore.path", sslTruststorePath);
      }
      if (sslTruststorePassword != null && !sslTruststorePassword.isEmpty()) {
        settingsBuilder = settingsBuilder.put("xpack.ssl.truststore.password", sslTruststorePassword);
      }
    }
    if (useElasticCloud) {
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
    TransportClient client;
    if (useSecurity) {
      client = new PreBuiltXPackTransportClient(settings, XPackPlugin.class);
    } else {
      client = new PreBuiltXPackTransportClient(settings);
    }
    return client.addTransportAddresses(elasticAddresses);
  }

  @Override
  public Node createTestNode(Map<String, Object> configs) {
    Settings.Builder settings = Settings.builder();
    for (Map.Entry<String, Object> config : configs.entrySet()) {
      settings.put(config.getKey(), config.getValue());
    }
    return new TestNode(settings.build());
  }

  static class TestNode extends Node {
    public TestNode(Settings settings) {
      super(
          InternalSettingsPreparer.prepareEnvironment(settings, null),
          // To enable an http port in integration tests, the following plugins must be loaded.
          Arrays.asList(Netty3Plugin.class, Netty4Plugin.class)
      );
    }
  }
}
