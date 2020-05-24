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
import com.streamsets.datacollector.util.Version;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.pipeline.api.Config;
import org.apache.commons.lang3.StringUtils;
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

    // Check that this pipeline wasn't created by a higher version
    // SDC version was only added in 3.7.0 to the pipeline bean, so for really ancient pipelines, this field wouldn't
    // exists. But that is actually fine for purpose of this check - those old pipelines are always created on SDC
    // version lower then this one.
    if(!StringUtils.isEmpty(fragmentConfiguration.getInfo().getSdcVersion())) {
      Version createdVersion = new Version(fragmentConfiguration.getInfo().getSdcVersion());
      if (createdVersion.isGreaterThan(buildInfo.getVersion())) {
        LOG.error("Validation failed: {} vs {}", fragmentConfiguration.getInfo().getSdcVersion(), buildInfo.getVersion());
        IssueCreator issueCreator = IssueCreator.getPipeline();
        issues.add(issueCreator.create(
            ValidationError.VALIDATION_0096,
            fragmentConfiguration.getInfo().getSdcVersion(),
            buildInfo.getVersion()
        ));

        // In case that the version is higher then the one we can work with, we don't upgrade the rest (too many
        // unrelated errors would be displayed in that case).
        return null;
      }
    }

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

    // And lastly update the SDC version (since the pipeline was just changed)
    if(issues.isEmpty()) {
      fragmentConfiguration.getInfo().setSdcVersion(buildInfo.getVersion());
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
