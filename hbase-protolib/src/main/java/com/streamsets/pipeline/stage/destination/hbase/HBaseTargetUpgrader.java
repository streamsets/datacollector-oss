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
package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class HBaseTargetUpgrader implements StageUpgrader {
  private static final String HBASE_CONNECTION_CONFIG = "hBaseConnectionConfig";
  private static final Joiner joiner = Joiner.on(".");

  private static final String ZOOKEEPER_QUORUM = "zookeeperQuorum";
  private static final String CLIENT_PORT = "clientPort";
  private static final String ZOOKEEPER_PARENT_ZNODE_OLD = "zookeeperParentZnode";
  private static final String ZOOKEEPER_PARENT_ZNODE = "hBaseConnectionConfig.zookeeperParentZNode";
  private static final String TABLE_NAME = "tableName";
  private static final String KERBEROS_AUTH = "kerberosAuth";
  private static final String HBASE_USER = "hbaseUser";
  private static final String HBASE_CONF_DIR = "hbaseConfDir";
  private static final String HBASE_CONFIGS = "hbaseConfigs";
  private static final String VALIDATE_TABLE_EXISTENCE = "validateTableExistence";

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
      List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        if (toVersion == 2) {
          break;
        }
        // fall through
      case 2:
        upgradeV2toV3(configs);
        if (toVersion == 3) {
          break;
        }
        // fall through
      case 3:
        upgradeV3toV4(configs);
        break;
        // fall through
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case ZOOKEEPER_QUORUM:
        case CLIENT_PORT:
        case ZOOKEEPER_PARENT_ZNODE_OLD:
        case TABLE_NAME:
        case KERBEROS_AUTH:
        case HBASE_USER:
        case HBASE_CONF_DIR:
        case HBASE_CONFIGS:
          configsToAdd.add(new Config(joiner.join(HBASE_CONNECTION_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          //no-op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }

  private static void upgradeV2toV3(List<Config> configs) {
    Config oldZnodeConfig = null;
    for (Config config : configs) {
      if (config.getName().contains(ZOOKEEPER_PARENT_ZNODE_OLD)) {
        oldZnodeConfig = config;
        break;
      }
    }

    if (oldZnodeConfig != null) {
      configs.add(new Config(ZOOKEEPER_PARENT_ZNODE, oldZnodeConfig.getValue()));
      configs.remove(oldZnodeConfig);
    }
  }

  private static void upgradeV3toV4(List<Config> configs) {
    configs.add(new Config(VALIDATE_TABLE_EXISTENCE, true));
  }
}
