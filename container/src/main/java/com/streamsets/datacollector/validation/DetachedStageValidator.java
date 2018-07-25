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
package com.streamsets.datacollector.validation;

import com.streamsets.datacollector.config.DetachedStageConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.configupgrade.PipelineConfigurationUpgrader;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DetachedStageValidator {

  protected final Issues issues;
  private StageLibraryTask stageLibraryTask;
  private DetachedStageConfiguration stageConf;

  public DetachedStageValidator(
    StageLibraryTask stageLibrary,
    DetachedStageConfiguration detachedStageConfiguration
  ) {
    this.stageLibraryTask = stageLibrary;
    this.stageConf = detachedStageConfiguration;
    this.issues = new Issues();
  }

  public DetachedStageConfiguration validate() {
    ValidationUtil.resolveLibraryAliases(stageLibraryTask, Collections.singletonList(stageConf.getStageConfiguration()));
    upgrade();

    // If there are any issues until this point, it does not make sense to continue
    if(issues.hasIssues()) {
      return stageConf;
    }

    ValidationUtil.addMissingConfigsToStage(stageLibraryTask, stageConf.getStageConfiguration());
    validateStageConfiguration();

    return stageConf;
  }

  private void upgrade() {
    List<Issue> issues = new ArrayList<>();

    StageConfiguration upgradeConf = PipelineConfigurationUpgrader.get().upgradeIfNecessary(
      stageLibraryTask,
      stageConf.getStageConfiguration(),
      issues
    );
    this.issues.addAll(issues);

    if(upgradeConf != null) {
      this.stageConf = new DetachedStageConfiguration(upgradeConf);
    }

    stageConf.setValidation(this);
  }

  private void validateStageConfiguration() {
    List<Issue> errors = new ArrayList<>();
    ValidationUtil.validateStageConfiguration(
      stageLibraryTask,
      false,
      stageConf.getStageConfiguration(),
      true,
      IssueCreator.getStage(stageConf.getStageConfiguration().getInstanceName()),
      false,
      Collections.emptyMap(),
      errors
    );
    this.issues.addAll(errors);
  }

  public Issues getIssues() {
    return issues;
  }
}
