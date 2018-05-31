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
package com.streamsets.datacollector.cluster;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.api.ExecutionMode;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ClusterProviderSelector implements ClusterProvider {
  private final RuntimeInfo runtimeInfo;
  private final SecurityConfiguration securityConfiguration;
  private final Configuration configuration;

  public ClusterProviderSelector(
      RuntimeInfo runtimeInfo, SecurityConfiguration securityConfiguration, Configuration configuration
  ) {
    this.runtimeInfo = runtimeInfo;
    this.securityConfiguration = securityConfiguration;
    this.configuration = configuration;
  }

  ClusterProvider getProvider(PipelineConfiguration pipelineConfiguration) {
    ExecutionMode executionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConfiguration, new ArrayList<>());
    switch (executionMode) {
      case CLUSTER_BATCH:
      case CLUSTER_YARN_STREAMING:
      case CLUSTER_MESOS_STREAMING:
        return new ShellClusterProvider(runtimeInfo, securityConfiguration, configuration);
      case EMR_BATCH:
        return new EmrClusterProvider(runtimeInfo, securityConfiguration, configuration);
      default:
        throw new IllegalArgumentException(String.format("Unexpected executionMode '%s'", executionMode));
    }
  }

  @Override
  public void killPipeline(
      File tempDir, String appId, PipelineConfiguration pipelineConfiguration
  ) throws TimeoutException, IOException {
    getProvider(pipelineConfiguration).killPipeline(
        tempDir,
        appId,
        pipelineConfiguration
    );
  }

  @Override
  public ClusterPipelineStatus getStatus(
      File tempDir, String appId, PipelineConfiguration pipelineConfiguration
  ) throws TimeoutException, IOException {
    return getProvider(pipelineConfiguration).getStatus(
        tempDir,
        appId,
        pipelineConfiguration
    );
  }

  @Override
  public ApplicationState startPipeline(
      File tempDir, Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration, PipelineConfigBean pipelineConfigBean, StageLibraryTask stageLibrary,
      CredentialStoresTask credentialStoresTask,
      File etcDir,
      File resourcesDir,
      File staticWebDir,
      File bootstrapDir,
      URLClassLoader apiCL,
      URLClassLoader containerCL,
      long timeToWaitForFailure,
      RuleDefinitions ruleDefinitions,
      Acl acl
  ) throws TimeoutException, IOException {
    return getProvider(pipelineConfiguration).startPipeline(
        tempDir, sourceInfo,
        pipelineConfiguration, pipelineConfigBean, stageLibrary,
        credentialStoresTask,
        etcDir,
        resourcesDir,
        staticWebDir,
        bootstrapDir,
        apiCL,
        containerCL,
        timeToWaitForFailure,
        ruleDefinitions,
        acl
    );
  }
}
