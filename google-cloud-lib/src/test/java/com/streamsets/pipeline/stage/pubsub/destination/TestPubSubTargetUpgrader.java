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

package com.streamsets.pipeline.stage.pubsub.destination;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.upgrade.UpgraderTestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestPubSubTargetUpgrader {

  private static final String PUB_SUB_TARGET_CONFIG = "conf";

  @Test
  public void testUpgrade() throws StageException {
    List<Config> configs = new ArrayList<>();

    PubSubTargetUpgrader pubSubTargetUpgrader = new PubSubTargetUpgrader();
    List<Config> upgradedConfigs = pubSubTargetUpgrader.upgrade("library", "stage", "stageInst", 1, 2, configs);

    Joiner p = Joiner.on(".");

    UpgraderTestUtils.assertAllExist(upgradedConfigs,
        p.join(PUB_SUB_TARGET_CONFIG, "requestBytesThreshold"),
        p.join(PUB_SUB_TARGET_CONFIG, "elementsCountThreshold"),
        p.join(PUB_SUB_TARGET_CONFIG, "defaultDelayThreshold"),
        p.join(PUB_SUB_TARGET_CONFIG, "batchingEnabled"),
        p.join(PUB_SUB_TARGET_CONFIG, "maxOutstandingElementCount"),
        p.join(PUB_SUB_TARGET_CONFIG, "maxOutstandingRequestBytes"),
        p.join(PUB_SUB_TARGET_CONFIG, "limitExceededBehavior"));
  }
}