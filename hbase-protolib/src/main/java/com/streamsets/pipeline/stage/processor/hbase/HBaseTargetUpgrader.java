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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.processor.hbase;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class HBaseTargetUpgrader implements StageUpgrader {
  private static final String DATA_FORMAT_CONFIG= "hBaseConnectionConfig";
  private static final Joiner joiner = Joiner.on(".");

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
      List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    final String ZOOKEEPERQUORUM = "zookeeperQuorum";
    final String CLIENTPORT = "clientPort";
    final String ZOOKEEPERPARENTZNODE = "zookeeperParentZnode";
    final String TABLENAME = "tableName";
    final String KERBEROSAUTH = "kerberosAuth";
    final String HBASEUSER = "hbaseUser";
    final String HBASECONFDIR = "hbaseConfDir";
    final String HBASECONFIGS = "hbaseConfigs";

    for (Config config : configs) {
      switch (config.getName()) {
        case ZOOKEEPERQUORUM:
        case CLIENTPORT:
        case ZOOKEEPERPARENTZNODE:
        case TABLENAME:
        case KERBEROSAUTH:
        case HBASEUSER:
        case HBASECONFDIR:
        case HBASECONFIGS:
          configsToAdd.add(new Config(joiner.join(DATA_FORMAT_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          //no-op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }
}
