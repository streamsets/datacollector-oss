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
package com.streamsets.datacollector.configupgrade;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.upgrader.YamlStageUpgraderLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConnectionConfigurationUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionConfigurationUpgrader.class);

  private static final ConnectionConfigurationUpgrader UPGRADER = new ConnectionConfigurationUpgrader() {
  };

  public static ConnectionConfigurationUpgrader get() {
    return UPGRADER;
  }

  protected ConnectionConfigurationUpgrader() {
  }

  /**
   * Upgrade a Connection, if necessary.
   *
   * @param connectionDef The ConnectionDef annotation for this Connection type
   * @param connectionConfiguration The Connection to upgrade
   * @param connectionId The ID of this Connection
   * @param issues Issues
   */
  public void upgradeIfNecessary(
      ConnectionDef connectionDef,
      ConnectionConfiguration connectionConfiguration,
      String connectionId,
      List<Issue> issues
  ) {
    ConnectionUpgradeContext upgradeContext = new ConnectionUpgradeContext(
        connectionConfiguration.getType(),
        connectionId,
        connectionConfiguration.getVersion(),
        connectionDef.version(),
        connectionDef.upgraderDef()
    );
    upgradeIfNecessary(upgradeContext, connectionConfiguration, issues);
  }

  /**
   * Upgrade a Connection, if necessary.
   *
   * @param libraryTask Stage Library
   * @param connectionConfiguration The Connection to upgrade
   * @param issues Issues
   */
  public void upgradeIfNecessary(
      StageLibraryTask libraryTask,
      ConnectionConfiguration connectionConfiguration,
      List<Issue> issues
  ) {
    ConnectionDefinition connDef = libraryTask.getConnection(connectionConfiguration.getType());
    if (connDef != null) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(connDef.getClassLoader());
        LOG.info("Upgrading connection '{}' from version '{}' to version '{}'",
            connDef.getName(), connectionConfiguration.getVersion(), connDef.getVersion());

        ConnectionUpgradeContext upgradeContext = new ConnectionUpgradeContext(
            connectionConfiguration.getType(),
            null,
            connectionConfiguration.getVersion(),
            connDef.getVersion(),
            connDef.getUpgrader()
        );
        upgradeIfNecessary(upgradeContext, connectionConfiguration, issues);
      } finally {
        Thread.currentThread().setContextClassLoader(cl);
      }
    }
  }

  /**
   * @param upgradeContext The upgrade context
   * @param connectionConfiguration The connection configuration to upgrade if necessary
   * @param issues The list of issues
   */
  private void upgradeIfNecessary(
      ConnectionUpgradeContext upgradeContext,
      ConnectionConfiguration connectionConfiguration,
      List<Issue> issues
  ) {
    Preconditions.checkArgument(issues.isEmpty(), "Given list of issues must be empty.");
    boolean upgrade = needsUpgrade(upgradeContext, issues);

    if (upgrade) {
      try {
        upgrade(upgradeContext, connectionConfiguration, issues);
      } catch (Exception ex) {
        LOG.error("Unknown exception during upgrade: " + ex, ex);
        issues.add(IssueCreator.getStage(upgradeContext.getStageName()).create(
            ContainerError.CONTAINER_0900,
            upgradeContext.getFromVersion(),
            upgradeContext.getToVersion(),
            ex.toString()
        ));
      }
    }
  }

  private void upgrade(
      ConnectionUpgradeContext upgradeContext,
      ConnectionConfiguration connectionConfiguration,
      List<Issue> issues
  ) {
    try {
      StageUpgrader upgrader = new YamlStageUpgraderLoader(
          connectionConfiguration.getType(),
          Thread.currentThread().getContextClassLoader().getResource(upgradeContext.getUpgraderDef())
      ).get();
      connectionConfiguration.setConfig(upgrader.upgrade(connectionConfiguration.getConfiguration(), upgradeContext));
      connectionConfiguration.setVersion(upgradeContext.toVersion);
    } catch (StageException ex) {
      issues.add(IssueCreator.getStage(upgradeContext.getStageName()).create(ex.getErrorCode(), ex.getParams()));
    }
  }

  private boolean needsUpgrade(
      ConnectionUpgradeContext upgradeContext,
      List<Issue> issues
  ) {
    return PipelineConfigurationUpgrader.needsUpgrade(
        upgradeContext.getToVersion(),
        upgradeContext.getFromVersion(),
        IssueCreator.getStage(upgradeContext.getStageName()),
        issues
    );
  }

  private static class ConnectionUpgradeContext implements StageUpgrader.Context {

    private final String type;
    private final String connectionId;
    private final int fromVersion;
    private final int toVersion;
    private final String upgraderDef;

    public ConnectionUpgradeContext(
        String type,
        String connectionId,
        int fromVersion,
        int toVersion,
        String upgraderDef
    ) {
      this.type = type;
      this.connectionId = connectionId;
      this.fromVersion = fromVersion;
      this.toVersion = toVersion;
      this.upgraderDef = upgraderDef;
    }

    @Override
    public String getLibrary() {
      return "";  // not used for Connections (YAML upgrader doesn't use it), but is required by StageUpgrader.Context
    }

    @Override
    public String getStageName() {
      return "Connection " + type;
    }

    @Override
    public String getStageInstance() {
      if (connectionId != null) {
        return getStageName() + " " + connectionId;
      }
      return getStageName();
    }

    @Override
    public int getFromVersion() {
      return fromVersion;
    }

    @Override
    public int getToVersion() {
      return toVersion;
    }

    public String getUpgraderDef() {
      return upgraderDef;
    }

    @Override
    public void registerService(Class service, List<Config> configs) {
      // noop
    }
  }
}
