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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticSearchDTargetUpgrader implements StageUpgrader {

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("timeDriver", "${time:now()}"));
    configs.add(new Config("timeZoneID", "UTC"));
  }

  @SuppressWarnings("unchecked")
  private void upgradeV2ToV3(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "clusterName":
        case "uris":
        case "timeDriver":
        case "timeZoneID":
        case "indexTemplate":
        case "typeTemplate":
        case "docIdTemplate":
        case "charset":
          configsToAdd.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + config.getName(), config.getValue()));
          configsToRemove.add(config);
          break;
        case "configs":
          // Remove client.transport.sniff from the additional configs when loading an old pipeline.
          // This config is disabled by default, and it should be enabled only when the user explicitly
          // checks a new checkbox in the UI.
          List<Map<String, String>> keyValues = (List<Map<String, String>>) config.getValue();
          Map<String, String> clientSniffKeyValue = null;
          for (Map<String, String> keyValue : keyValues) {
            for (Map.Entry entry : keyValue.entrySet()) {
              if (entry.getValue().equals("client.transport.sniff")) {
                clientSniffKeyValue = keyValue;
              }
            }
          }
          if (clientSniffKeyValue != null) {
            keyValues.remove(clientSniffKeyValue);
          }
          configsToAdd.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + config.getName(), keyValues));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config(ElasticSearchConfigBean.CONF_PREFIX + "httpUri", "hostname:port"));
  }

}
