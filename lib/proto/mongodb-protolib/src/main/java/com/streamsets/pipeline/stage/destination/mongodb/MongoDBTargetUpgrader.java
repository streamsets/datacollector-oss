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
package com.streamsets.pipeline.stage.destination.mongodb;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.mongodb.AuthenticationType;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;

import java.util.ArrayList;
import java.util.List;

public class MongoDBTargetUpgrader implements StageUpgrader {
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
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2toV3(configs);
        // fall through
      case 3:
        upgradeV3toV4(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "mongoTargetConfigBean.mongoClientURI":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "connectionString", config.getValue()));
          configsToRemove.add(config);
          break;
        case "mongoTargetConfigBean.database":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "database", config.getValue()));
          configsToRemove.add(config);
          break;
        case "mongoTargetConfigBean.collection":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "collection", config.getValue()));
          configsToRemove.add(config);
          break;
        case "mongoTargetConfigBean.uniqueKeyField":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "uniqueKeyField", config.getValue()));
          configsToRemove.add(config);
          break;
        case "mongoTargetConfigBean.writeConcern":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "writeConcern", config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          break;
      }
    }

    // new configs
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "authenticationType", AuthenticationType.NONE));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "username", ""));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "password", ""));

    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "connectionsPerHost", 100));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "minConnectionsPerHost", 0));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "connectTimeout", 10000));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "maxConnectionIdleTime", 0));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "maxConnectionLifeTime", 0));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "maxWaitTime", 120000));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "serverSelectionTimeout", 30000));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "threadsAllowedToBlockForConnectionMultiplier", 5));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "heartbeatFrequency", 10000));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "minHeartbeatFrequency", 500));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "heartbeatConnectTimeout", 20000));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "heartbeatSocketTimeout", 20000));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "localThreshold", 15));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "requiredReplicaSetName", ""));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "cursorFinalizerEnabled", true));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "socketKeepAlive", false));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "socketTimeout", 0));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "sslEnabled", false));
    configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "sslInvalidHostNameAllowed", false));

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private void upgradeV2toV3(List<Config> configs) {
    configs.add(new Config(MongoDBConfig.CONFIG_PREFIX + "isUpsert", false));
  }

  private void upgradeV3toV4(List<Config> configs) {
    configs.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "authSource", ""));
  }
}
