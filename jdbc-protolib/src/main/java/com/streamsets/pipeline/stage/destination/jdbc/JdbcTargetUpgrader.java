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
package com.streamsets.pipeline.stage.destination.jdbc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JDBCOperationType;
import com.streamsets.pipeline.lib.jdbc.JdbcBaseUpgrader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** {@inheritDoc} */
public class JdbcTargetUpgrader extends JdbcBaseUpgrader{
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTargetUpgrader.class);

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1toV2(configs);
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
        if (toVersion == 4) {
          break;
        }
        // fall through
      case 4:
        upgradeV4toV5(configs);
        if (toVersion == 5) {
          break;
        }
        // fall through
      case 5:
        upgradeV5toV6(configs);
        if(toVersion == 6) {
          break;
        }
        // fall through
      case 6:
        upgradeV6toV7(configs);
        if(toVersion == 7) {
          break;
        }
        //fall through
      case 7:
        //fall through
      case 8:
        //fall through
        //We bumped the Sql server source to 10, so making it consistent
      case 9:
        upgradeV8ToV9(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }


  private void upgradeV4toV5(List<Config> configs) {
    LOG.info("Upgrading from 4 to 5");
    // added new max parameters feature - set to default
    configs.add(new Config("maxPrepStmtParameters", -1));
  }

  @SuppressWarnings("unchecked")
  private void upgradeV1toV2(List<Config> configs) {
    LOG.info("Upgrading from 1 to 2");
    configs.add(new Config("changeLogFormat", "NONE"));

    Config tableNameConfig = null;
    for (Config config : configs) {
      if (config.getName().equals("qualifiedTableName")) {
        tableNameConfig = config;
        break;
      }
    }

    if (null != tableNameConfig) {
      configs.add(new Config("tableName", tableNameConfig.getValue()));
      configs.remove(tableNameConfig);
    }

    for (Config config : configs) {
      if (config.getName().equals("columnNames")) {
        for (HashMap<String, String> columnName : (List<HashMap<String, String>>) config.getValue()) {
          columnName.put("paramValue", "?");
        }
      }
    }
  }

  private void upgradeV2toV3(List<Config> configs) {
    LOG.info("Upgrading from 2 to 3");
    configs.add(new Config("useMultiRowInsert", true));
  }

  private void upgradeV3toV4(List<Config> configs) {
    LOG.info("Upgrading from 3 to 4");
    upgradeToConfigBeanV1(configs);

    Config tableNameConfig = null;
    Config readOnlyConfig = null;
    for (Config config : configs) {
      if (config.getName().equals("tableName")) {
        tableNameConfig = config;
      }
      if (config.getName().equals("hikariConfigBean.readOnly")) {
        readOnlyConfig = config;
      }
    }

    // Rename tableName to tableNameTemplate
    if (null != tableNameConfig) {
      configs.add(new Config("tableNameTemplate", tableNameConfig.getValue()));
      configs.remove(tableNameConfig);
    }

    // Remove hikariConfigBean.readOnly
    if (null != readOnlyConfig) {
      configs.remove(readOnlyConfig);
    }

  }

  private void upgradeV5toV6(List<Config> configs) {
    LOG.info("Upgrading from 5 to 5");
    // added default operation, unsupported operation action, and maxPrepStmtCache
    configs.add(new Config("defaultOperation", "INSERT"));
    configs.add(new Config("unsupportedAction", "DISCARD"));
    configs.add(new Config("maxPrepStmtCache", -1));
  }

  private void upgradeV6toV7(List<Config> configs) {
    LOG.info("Upgrading from 6 to 7");
    Config maxPrepStmtCache = null;
    for (Config config : configs) {
      if (config.getName().equals("maxPrepStmtCache")) {
        maxPrepStmtCache = config;
        break;
      }
    }

    if(maxPrepStmtCache != null) {
      configs.remove(maxPrepStmtCache);
    }
  }

  private void upgradeV8ToV9(List<Config> configs) {
    LOG.info("Upgrading from 8 to 9");
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();
    for (Config config : configs) {
     if (config.getName().equals("defaultOperation") && config.getValue() == null) {
       configsToRemove.add(config);
       configsToAdd.add(new Config(config.getName(), JDBCOperationType.INSERT));
     }
    }
    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

}
