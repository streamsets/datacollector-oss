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
package com.streamsets.datacollector.creation;

import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.configupgrade.RuleDefinitionsUpgrader;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestRuleDefinitionsUpgrader {

  @Test
  public void testRuleDefinitionsUpgrader() {
    RuleDefinitionsUpgrader ruleDefinitionsUpgrader = RuleDefinitionsUpgrader.get();

    RuleDefinitions upgradedRulesDefinitions = ruleDefinitionsUpgrader.upgradeIfNecessary(
        "samplePipelineId",
        getSchemaVersion2RuleDefinitions(),
        Collections.emptyList()
    );
    Assert.assertNotNull(upgradedRulesDefinitions);
    Assert.assertNotNull(upgradedRulesDefinitions.getConfiguration());
    Assert.assertEquals(1, upgradedRulesDefinitions.getConfiguration().size());
    Assert.assertEquals("emailIDs", upgradedRulesDefinitions.getConfiguration().get(0).getName());
    List<String> emailIds = (List<String>)upgradedRulesDefinitions.getConfiguration().get(0).getValue();
    Assert.assertNotNull(emailIds);
    Assert.assertEquals("sample1@email.com", emailIds.get(0));
    Assert.assertEquals("sample2@email.com", emailIds.get(1));
  }

  private RuleDefinitions getSchemaVersion2RuleDefinitions() {
    return new RuleDefinitions(
        2,
        RuleDefinitionsConfigBean.VERSION,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        ImmutableList.of("sample1@email.com", "sample2@email.com"),
        UUID.randomUUID(),
        new ArrayList<>()
    );
  }

}
