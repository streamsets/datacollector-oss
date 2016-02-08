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
package com.streamsets.pipeline.elasticsearch.api;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public abstract class ElasticSearchFactory {

  private static ServiceLoader<ElasticSearchFactory> factoriesLoader = ServiceLoader.load(ElasticSearchFactory.class);
  private static ElasticSearchFactory factory;

  public abstract Client createClient(
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
  ) throws UnknownHostException;

  public abstract Settings createSettings(Map<String, Object> configs);

  static {
    int serviceCount = 0;
    for (ElasticSearchFactory factory : factoriesLoader) {
      ElasticSearchFactory.factory = factory;
      serviceCount++;
    }
    if (serviceCount == 0) {
      throw new RuntimeException("No implementation of ElasticSearchFactory is found");
    }
    if (serviceCount > 1) {
      throw new RuntimeException("There can only be one implementation of ElasticSearchFactory");
    }
  }

  public static Client client(
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
    return factory.createClient(
        clusterName,
        uris,
        clientSniff,
        configs,
        useShield,
        shieldUser,
        shieldTransportSsl,
        sslKeystorePath,
        sslKeystorePassword,
        useFound
    );
  }

  public static Settings settings(Map<String, Object> configs) {
    return factory.createSettings(configs);
  }
}
