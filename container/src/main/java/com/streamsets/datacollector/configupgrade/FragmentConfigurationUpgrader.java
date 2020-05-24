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
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.pipeline.api.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FragmentConfigurationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationUpgrader.class);

  private static final FragmentConfigurationUpgrader UPGRADER = new FragmentConfigurationUpgrader() {};

  public static FragmentConfigurationUpgrader get() {
    return UPGRADER;
  }

  protected FragmentConfigurationUpgrader() {
  }

  public PipelineFragmentConfiguration upgradeIfNecessary(
      StageLibraryTask stageLibrary,
      BuildInfo buildInfo,
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

    // Upgrading data if needed
    upgrade = needsUpgrade(stageLibrary, fragmentConfiguration, issues);
    if (upgrade && issues.isEmpty()) {
      fragmentConfiguration = upgrade(stageLibrary, fragmentConfiguration, issues);
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

  private PipelineFragmentConfiguration upgrade(
      StageLibraryTask library,
      PipelineFragmentConfiguration fragmentConfiguration,
      List<Issue> issues
  ) {
    // upgrade pipeline fragment level configs if necessary
    StageConfiguration fragmentConfAsStageConf = PipelineBeanCreator.getPipelineConfAsStageConf(fragmentConfiguration);
    if (PipelineConfigurationUpgrader.needsUpgrade(
        library,
        getFragmentDefinition(),
        fragmentConfAsStageConf,
        issues
    )) {
      PipelineConfigurationUpgrader.upgradeIfNeeded(
          library,
          getFragmentDefinition(),
          fragmentConfAsStageConf,
          issues
      );
    }

    List<Issue> ownIssues = new ArrayList<>();
    List<StageConfiguration> stageInstances = new ArrayList<>();

    // Test origin
    StageConfiguration testOrigin = fragmentConfiguration.getTestOriginStage();
    if (testOrigin != null) {
      testOrigin = PipelineConfigurationUpgrader.upgradeIfNeeded(library, testOrigin, ownIssues);
    }

    // upgrade stages;
    for (StageConfiguration stageConf : fragmentConfiguration.getStages()) {
      stageConf = PipelineConfigurationUpgrader.upgradeIfNeeded(library, stageConf, ownIssues);
      stageInstances.add(stageConf);
    }


    // if ownIssues > 0 we had an issue upgrading, we wont touch the pipelineConf and return null
    if (ownIssues.isEmpty()) {
      fragmentConfiguration.setTestOriginStage(testOrigin);
      fragmentConfiguration.setStages(stageInstances);

      if (testOrigin != null) {
        fragmentConfAsStageConf.addConfig(new Config(
            "testOriginStage",
            PipelineConfigurationUpgrader.stageToUISelect(testOrigin))
        );
      }

      fragmentConfiguration.setConfiguration(fragmentConfAsStageConf.getConfiguration());
      fragmentConfiguration.setVersion(fragmentConfAsStageConf.getStageVersion());
    } else {
      issues.addAll(ownIssues);
      fragmentConfiguration = null;
    }

    return fragmentConfiguration;
  }

  boolean needsUpgrade(
      StageLibraryTask library,
      PipelineFragmentConfiguration fragmentConfiguration,
      List<Issue> issues
  ) {
    boolean upgrade;

    // pipeline fragment configurations
    StageConfiguration pipelineConfs = PipelineBeanCreator.getPipelineConfAsStageConf(fragmentConfiguration);
    upgrade = PipelineConfigurationUpgrader.needsUpgrade(library, getFragmentDefinition(), pipelineConfs, issues);

    // Test origin
    if (fragmentConfiguration.getTestOriginStage() != null) {
      upgrade |= PipelineConfigurationUpgrader.needsUpgrade(
          library,
          fragmentConfiguration.getTestOriginStage(),
          issues
      );
    }

    // pipeline stages configurations
    for (StageConfiguration conf : fragmentConfiguration.getStages()) {
      upgrade |= PipelineConfigurationUpgrader.needsUpgrade(library, conf, issues);
    }

    return upgrade;
  }
}
