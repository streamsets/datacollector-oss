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
package com.streamsets.pipeline.stage.processor.lookup;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.salesforce.LookupMode;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ForceLookupProcessorUpgrader implements StageUpgrader {
  private static final String CACHE_CONFIG = "cacheConfig";

  @Override
  public List<Config> upgrade(List<Config> configs, Context context) throws StageException {
    int fromVersion = context.getFromVersion();

    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private static void upgradeV1ToV2(List<Config> configs) {
    // RETRIEVE lookup mode was added without an upgrader, so default to QUERY
    String lookupMode = LookupMode.QUERY.name();

    // Default behavior differed according to the lookup mode:
    // QUERY lookups sent missing records to error
    // RETRIEVE lookups passed missing records on
    for (Config config : configs) {
      if (config.getName().equals("forceConfig.lookupMode")) {
        lookupMode = config.getValue().toString();
        break;
      }
    }
    configs.add(new Config("forceConfig.missingValuesBehavior",
        (lookupMode.equals(LookupMode.QUERY.name())) ? MissingValuesBehavior.SEND_TO_ERROR : MissingValuesBehavior.PASS_RECORD_ON));
  }
}
