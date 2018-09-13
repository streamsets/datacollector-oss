/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.pulsar;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestPulsarTargetUpgrader {

  @Test
  public void testUpgradeV1toV2() throws StageException {
    List<Config> configs = new ArrayList<>();

    PulsarTargetUpgrader pulsarTargetUpgrader = new PulsarTargetUpgrader();
    List<Config> upgradedConfigs = pulsarTargetUpgrader.upgrade("library", "stage", "stageInst", 1, 2, configs);

    UpgraderTestUtils.assertAllExist(upgradedConfigs,
        "pulsarConfig.maxPendingMessages",
        "pulsarConfig.asyncSend",
        "pulsarConfig.enableBatching",
        "pulsarConfig.batchMaxMessages",
        "pulsarConfig.batchMaxPublishDelay");
  }

}
