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
package com.streamsets.datacollector.configupgrade;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.datacollector.validation.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FragmentConfigurationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationUpgrader.class);

  private static final FragmentConfigurationUpgrader UPGRADER = new FragmentConfigurationUpgrader() {};

  public static FragmentConfigurationUpgrader get() {
    return UPGRADER;
  }

  private FragmentConfigurationUpgrader() {
  }

  public PipelineFragmentConfiguration upgradeIfNecessary(
      PipelineFragmentConfiguration fragmentConfiguration,
      List<Issue> issues
  ) {
    Preconditions.checkArgument(issues.size() == 0, "Given list of issues must be empty.");
    boolean upgrade;

    // Firstly upgrading schema if needed, then data
    upgrade = needsSchemaUpgrade(fragmentConfiguration);
    if(upgrade && issues.isEmpty()) {
      fragmentConfiguration = upgradeSchema(fragmentConfiguration, issues);
    }

    if(issues.isEmpty()) {
      upgrade(fragmentConfiguration, issues);
    }

    return (issues.isEmpty()) ? fragmentConfiguration : null;
  }

  private boolean needsSchemaUpgrade(PipelineFragmentConfiguration ruleDefinitions) {
    return ruleDefinitions.getSchemaVersion() != PipelineStoreTask.FRAGMENT_SCHEMA_VERSION;
  }

  private PipelineFragmentConfiguration upgradeSchema(
      PipelineFragmentConfiguration fragmentConfiguration,
      List<Issue> issues
  ) {
    LOG.debug("Upgrading schema from version {} for pipeline fragment {}",
        fragmentConfiguration.getSchemaVersion(), fragmentConfiguration.getPipelineId());
    switch (fragmentConfiguration.getSchemaVersion()) {
      case 0:
      case 1:
        upgradeSchema2to3(fragmentConfiguration, issues);
        break;
      default:
        issues.add(IssueCreator.getStage(null)
            .create(ValidationError.VALIDATION_0000, fragmentConfiguration.getSchemaVersion()));
    }

    fragmentConfiguration.setSchemaVersion(PipelineStoreTask.FRAGMENT_SCHEMA_VERSION);
    return issues.isEmpty() ? fragmentConfiguration : null;
  }

  private void upgradeSchema2to3(PipelineFragmentConfiguration fragmentConfiguration, List<Issue> issues) {
    // Added new attributes:
    // * testOriginStage
    fragmentConfiguration.setTestOriginStage(null);
  }

  private StageDefinition getFragmentDefinition() {
    return PipelineBeanCreator.FRAGMENT_DEFINITION;
  }

  private void upgrade(PipelineFragmentConfiguration fragmentConfiguration, List<Issue> issues) {
    StageConfiguration fragmentConfAsStageConf = PipelineBeanCreator.getPipelineConfAsStageConf(fragmentConfiguration);
    if (PipelineConfigurationUpgrader.needsUpgrade(
        null,
        getFragmentDefinition(),
        fragmentConfAsStageConf,
        issues
    )) {
      fragmentConfAsStageConf = PipelineConfigurationUpgrader.upgradeIfNeeded(
          null,
          getFragmentDefinition(),
          fragmentConfAsStageConf,
          issues
      );
      fragmentConfiguration.setConfiguration(fragmentConfAsStageConf.getConfiguration());
      fragmentConfiguration.setVersion(fragmentConfAsStageConf.getStageVersion());
    }
  }
}
