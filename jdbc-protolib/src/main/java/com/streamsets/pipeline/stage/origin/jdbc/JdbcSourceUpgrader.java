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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.upgrade.UpgraderUtils;
import com.streamsets.pipeline.lib.jdbc.JdbcBaseUpgrader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** {@inheritDoc} */
public class JdbcSourceUpgrader extends JdbcBaseUpgrader {

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        // fall through
      case 4:
        upgradeV4toV5(configs);
        // fall through
      case 5:
        upgradeV5toV6(configs);
        // fall through
      case 6:
        upgradeV6toV7(configs);
        // fall through
      case 7:
        upgradeV7toV8(configs);
        // fall through
      case 8:
        upgradeV8toV9(configs);
        if (toVersion == 9) {
          break;
        }
      case 9:
        upgradeV9toV10(configs);
        if (toVersion == 10) {
          break;
        }
        // fall through
      case 10:
        //fall through
      case 11:
        //We bumped the Sql server source to 11, so making it consistent
        upgradeV11ToV12(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }


  private void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("maxBatchSize", 1000));
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("txnIdColumnName", ""));
    configs.add(new Config("txnMaxSize", 10000));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("jdbcRecordType", "MAP"));
  }

  private void upgradeV4toV5(List<Config> configs) {
    configs.add(new Config("maxClobSize", 1000));
  }

  private void upgradeV5toV6(List<Config> configs) {
    upgradeToConfigBeanV1(configs);
  }

  private void upgradeV6toV7(List<Config> configs) {
    configs.add(new Config("createJDBCNsHeaders", false));
    configs.add(new Config("jdbcNsHeaderPrefix", "jdbc."));
  }

  private void upgradeV7toV8(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();
    Set<String> configsMoved = ImmutableSet.of("queryInterval", "maxBatchSize", "maxClobSize", "maxBlobSize");
    for (Config config : configs) {
      if (configsMoved.contains(config.getName())) {
        configsToRemove.add(config);
        configsToAdd.add(new Config(CommonSourceConfigBean.COMMON_SOURCE_CONFIG_BEAN_PREFIX + config.getName(), config.getValue()));
      }
    }
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

  private void upgradeV8toV9(List<Config> configs) {
    configs.add(new Config("disableValidation", false));
  }

  private void upgradeV9toV10(List<Config> configs) {
    UpgraderUtils.moveAllTo(configs, "commonSourceConfigBean.queryInterval", "queryInterval");
  }

  private void upgradeV11ToV12(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    for (Config config : configs) {
      if (config.getName().equals("jdbcRecordType")) {
        configsToRemove.add(config);
      }
    }
    configs.removeAll(configsToRemove);
  }

}
