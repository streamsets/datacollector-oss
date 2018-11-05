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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;

import java.util.ArrayList;
import java.util.List;

import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.CONNECTION_TIMEOUT_NAME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.DEFAULT_CONNECTION_TIMEOUT;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.DEFAULT_IDLE_TIMEOUT;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.DEFAULT_MAX_LIFETIME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.DEFAULT_MAX_POOL_SIZE;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.DEFAULT_MIN_IDLE;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.DEFAULT_READ_ONLY;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.HIKARI_BEAN_NAME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.IDLE_TIMEOUT_NAME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.MAX_LIFETIME_NAME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.MAX_POOL_SIZE_NAME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.MIN_IDLE_NAME;
import static com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean.READ_ONLY_NAME;

public abstract class JdbcBaseUpgrader implements StageUpgrader {
  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

  private void moveConfigToHikariBean(Config config) {
    configsToRemove.add(config);
    configsToAdd.add(new Config(HIKARI_BEAN_NAME + config.getName(), config.getValue()));
  }

  protected void upgradeToConfigBeanV1(List<Config> configs) {
    for (Config config : configs) {
      // Migrate existing configs that were moved into the HikariPoolConfigBean
      switch (config.getName()) {
        case "connectionString":
          // fall through
        case "useCredentials":
          // fall through
        case "username":
          // fall through
        case "password":
          // fall through
        case "driverProperties":
          // fall through
        case "driverClassName":
          // fall through
        case "connectionTestQuery":
          moveConfigToHikariBean(config);
          break;
        default:
          // no-op
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    // Add newly added configs
    configs.add(new Config(HIKARI_BEAN_NAME + MAX_POOL_SIZE_NAME, DEFAULT_MAX_POOL_SIZE));
    configs.add(new Config(HIKARI_BEAN_NAME + MIN_IDLE_NAME, DEFAULT_MIN_IDLE));
    configs.add(new Config(HIKARI_BEAN_NAME + CONNECTION_TIMEOUT_NAME, DEFAULT_CONNECTION_TIMEOUT));
    configs.add(new Config(HIKARI_BEAN_NAME + IDLE_TIMEOUT_NAME, DEFAULT_IDLE_TIMEOUT));
    configs.add(new Config(HIKARI_BEAN_NAME + MAX_LIFETIME_NAME, DEFAULT_MAX_LIFETIME));
    configs.add(new Config(HIKARI_BEAN_NAME + READ_ONLY_NAME, DEFAULT_READ_ONLY));
  }
}
