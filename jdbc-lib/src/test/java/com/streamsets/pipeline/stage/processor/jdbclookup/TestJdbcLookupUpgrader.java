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

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.processor.jdbclookup.JdbcLookupProcessorUpgrader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestJdbcLookupUpgrader {

  @Test
  public void testUpgradeV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();

    JdbcLookupProcessorUpgrader upgrader = new JdbcLookupProcessorUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 1, 2, configs);

    UpgraderTestUtils.assertAllExist(upgradedConfigs,
        "cacheConfig.enabled",
        "cacheConfig.maxSize",
        "cacheConfig.evictionPolicyType",
        "cacheConfig.expirationTime",
        "cacheConfig.timeUnit"
        );
  }

  @Test
  public void testUpgradeV2toV3() throws StageException {
    List<Config> configs = new ArrayList<>();
    final String queryIntervalField = "commonSourceConfigBean.queryInterval";
    final String queryInterval = "5";
    configs.add(new Config(queryIntervalField, queryInterval));

    JdbcLookupProcessorUpgrader upgrader = new JdbcLookupProcessorUpgrader();
    List<Config> upgradedConfigs = upgrader.upgrade("lib", "stage", "stageInst", 2, 3, configs);

    UpgraderTestUtils.assertExists(upgradedConfigs, "missingValuesBehavior", MissingValuesBehavior.SEND_TO_ERROR);
  }
}
