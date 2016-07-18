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
package com.streamsets.pipeline.stage.origin.http;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.http.AuthenticationType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HttpClientSourceUpgrader implements StageUpgrader {
  private static final String CONF = "conf";
  private static final String BASIC = "basic";
  private static final String AUTH_TYPE = "authType";
  private static final String OAUTH = "oauth";
  private static final String DATA_FORMAT_CONFIG= "dataFormatConfig";

  private static final Joiner joiner = Joiner.on(".");

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        if (toVersion == 5) {
          break;
        }
        // fall through
      case 5:
        upgradeV5ToV6(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "resourceUrl":
        case "httpMethod":
        case "requestBody":
        case "requestTimeoutMillis":
        case "httpMode":
        case "pollingInterval":
        case "entityDelimiter":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "isOAuthEnabled":
          AuthenticationType authType = AuthenticationType.NONE;
          if ((boolean)config.getValue()) {
            authType = AuthenticationType.OAUTH;
          }
          configsToAdd.add(new Config(joiner.join(CONF, "authType"), authType));
          configsToRemove.add(config);
          break;
        case "jsonMode":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "jsonContent"), config.getValue()));
          configsToRemove.add(config);
          break;
        case "consumerKey":
        case "consumerSecret":
        case "token":
        case "tokenSecret":
          configsToAdd.add(new Config(joiner.join(CONF, OAUTH, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "batchSize":
          configsToAdd.add(new Config(joiner.join(CONF, BASIC, "maxBatchSize"), config.getValue()));
          configsToRemove.add(config);
          break;
        case "maxBatchWaitTime":
          configsToAdd.add(new Config(joiner.join(CONF, BASIC, "maxWaitTime"), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, "csvSkipStartLines"), 0));

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("conf.useProxy", false));
    configs.add(new Config("conf.proxy.uri", ""));
    configs.add(new Config("conf.proxy.username", ""));
    configs.add(new Config("conf.proxy.password", ""));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    for (Config config : configs) {
      if (joiner.join(CONF, AUTH_TYPE)
          .equals(config.getName()) && AuthenticationType.BASIC.name() == config.getValue()) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(joiner.join(CONF, AUTH_TYPE), AuthenticationType.UNIVERSAL));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config("conf.headers", new HashMap<String, String>()));
  }

  private void upgradeV5ToV6(List<Config> configs) {
    for (Config config : configs) {
      if (joiner.join(CONF, "requestData").equals(config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(joiner.join(CONF, "requestBody"), config.getValue()));
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }
}
