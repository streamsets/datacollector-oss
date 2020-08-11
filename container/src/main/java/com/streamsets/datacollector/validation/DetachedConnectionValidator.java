/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.DetachedConnectionConfiguration;
import com.streamsets.datacollector.configupgrade.ConnectionConfigurationUpgrader;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.List;

public class DetachedConnectionValidator {

  protected final Issues issues;
  private final StageLibraryTask stageLibraryTask;
  private final DetachedConnectionConfiguration connectionConf;

  public DetachedConnectionValidator(
    StageLibraryTask stageLibrary,
    DetachedConnectionConfiguration DetachedConnectionConfiguration
  ) {
    this.stageLibraryTask = stageLibrary;
    this.connectionConf = DetachedConnectionConfiguration;
    this.issues = new Issues();
  }

  /**
   * @param forceUpgrade Whether to force upgrade the connection
   * @return A detached connection configuration:
   *  The original one if forceUpgrade=false or the upgraded connection if forceUpgrade=true
   */
  public DetachedConnectionConfiguration validate(boolean forceUpgrade) {
    ConnectionDefinition connDef = stageLibraryTask.getConnection(connectionConf.getConnectionConfiguration().getType());
    connectionConf.setLatestAvailableVersion(connDef.getVersion());
    List<Config> originalConfigs = new ArrayList<>();
    originalConfigs.addAll(connectionConf.getConnectionConfiguration().getConfiguration());
    int originalVersion = connectionConf.getConnectionConfiguration().getVersion();

    upgrade();

    // If there are any issues until this point, it does not make sense to continue
    if (issues.hasIssues()) {
      return connectionConf;
    }

    ValidationUtil.addMissingConfigsToConnection(stageLibraryTask, connectionConf.getConnectionConfiguration());
    validateConnectionConfiguration();

    if (!forceUpgrade) {
      // restore original version and configurations if we don't want to force upgrade the connection
      connectionConf.getConnectionConfiguration().setConfig(originalConfigs);
      connectionConf.getConnectionConfiguration().setVersion(originalVersion);
    }
    return connectionConf;
  }

  private void upgrade() {
    List<Issue> issues = new ArrayList<>();

    ConnectionConfigurationUpgrader.get().upgradeIfNecessary(
      stageLibraryTask,
      connectionConf.getConnectionConfiguration(),
      issues
    );
    this.issues.addAll(issues);

    connectionConf.setValidation(this);
  }

  private void validateConnectionConfiguration() {
    List<Issue> issues = new ArrayList<>();
    ValidationUtil.validateConnectionConfiguration(
      stageLibraryTask,
      connectionConf.getConnectionConfiguration(),
      IssueCreator.getStage("Connection " + connectionConf.getConnectionConfiguration().getType()),
      issues
    );
    this.issues.addAll(issues);
  }

  public Issues getIssues() {
    return issues;
  }
}
