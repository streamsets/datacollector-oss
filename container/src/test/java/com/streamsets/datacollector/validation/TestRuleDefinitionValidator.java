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
package com.streamsets.datacollector.validation;

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestRuleDefinitionValidator {

  @Test
  public void testValidConfiguration() {
    RuleDefinitions ruleDefinitions = new RuleDefinitions(
        FilePipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        Collections.<MetricsRuleDefinition>emptyList(),
        Collections.<DataRuleDefinition>emptyList(),
        Collections.<DriftRuleDefinition>emptyList(),
        null,
        UUID.randomUUID(),
        new ArrayList<>()
    );

    RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator(
        "pipelineId",
        ruleDefinitions,
        Collections.emptyMap()
    );

    Assert.assertTrue(ruleDefinitionValidator.validateRuleDefinition());
  }

  @Test
  public void testInValidConfiguration() {
    List<Config> rulesConfiguration = ImmutableList.of(
        new Config("emailIDs", ImmutableList.of("${USER_EMAIL_ID}"))
    );
    RuleDefinitions ruleDefinitions = new RuleDefinitions(
        FilePipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        Collections.<MetricsRuleDefinition>emptyList(),
        Collections.<DataRuleDefinition>emptyList(),
        Collections.<DriftRuleDefinition>emptyList(),
        null,
        UUID.randomUUID(),
        rulesConfiguration
    );

    RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator(
        "pipelineId",
        ruleDefinitions,
        Collections.emptyMap()
    );

    Assert.assertFalse(ruleDefinitionValidator.validateRuleDefinition());
    Assert.assertNotNull(ruleDefinitions.getConfigIssues());
    Assert.assertEquals(1, ruleDefinitions.getConfigIssues().size());
    Assert.assertEquals("emailIDs", ruleDefinitions.getConfigIssues().get(0).getConfigName());
    Assert.assertTrue(
        ruleDefinitions.getConfigIssues().get(0).getMessage().contains("'USER_EMAIL_ID' cannot be resolved")
    );

    ruleDefinitionValidator = new RuleDefinitionValidator(
        "pipelineId",
        ruleDefinitions,
        ImmutableMap.of("USER_EMAIL_ID", "user1@cmp1.com")
    );

    Assert.assertTrue(ruleDefinitionValidator.validateRuleDefinition());
    Assert.assertNotNull(ruleDefinitions.getConfigIssues());
    Assert.assertEquals(0, ruleDefinitions.getConfigIssues().size());
  }
}
