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
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JdbcLookupProcessorUpgrader implements StageUpgrader {
  private static final String CACHE_CONFIG = "cacheConfig";

  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      case 2:
        upgradeV2ToV3(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    Joiner p = Joiner.on(".");
    configs.add(new Config(p.join(CACHE_CONFIG, "enabled"), false));
    configs.add(new Config(p.join(CACHE_CONFIG, "maxSize"), -1));
    configs.add(new Config(p.join(CACHE_CONFIG, "evictionPolicyType"), null));
    configs.add(new Config(p.join(CACHE_CONFIG, "expirationTime"), 1));
    configs.add(new Config(p.join(CACHE_CONFIG, "timeUnit"), TimeUnit.SECONDS));
  }

  private static void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("missingValuesBehavior", MissingValuesBehavior.SEND_TO_ERROR));
  }
}
