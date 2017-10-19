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
package com.streamsets.datacollector.execution.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.cluster.ApplicationState;
import com.streamsets.datacollector.cluster.ClusterPipelineStatus;
import com.streamsets.datacollector.cluster.ClusterProvider;
import com.streamsets.datacollector.cluster.ClusterProviderImpl;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.SystemProcessFactory;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Helper class to interact with the underlying cluster frameworks like Spark, MR to start, kill or check the
 * status of job.
 */
public class ClusterHelper {
  private final SystemProcessFactory systemProcessFactory;
  private final ClusterProvider clusterProvider;
  private final File tempDir;
  private final File clusterManagerFile;
  private URLClassLoader apiCL;
  private URLClassLoader containerCL;
  private Configuration configuration;

  public ClusterHelper(RuntimeInfo runtimeInfo, SecurityConfiguration securityConfiguration,
                       File tempDir, Configuration conf) {
    this(new SystemProcessFactory(), new ClusterProviderImpl(runtimeInfo, securityConfiguration, conf), tempDir, new File(
      runtimeInfo.getLibexecDir(), "_cluster-manager"), null, null, securityConfiguration);
  }

  @VisibleForTesting
  public ClusterHelper(SystemProcessFactory systemProcessFactory, ClusterProvider clusterProvider, File tempDir,
    File clusterManagerFile, URLClassLoader apiCL, URLClassLoader containerCL,
    SecurityConfiguration securityConfiguration) {
    this.systemProcessFactory = systemProcessFactory;
    this.clusterProvider = clusterProvider;
    this.tempDir = tempDir;
    this.clusterManagerFile = clusterManagerFile;
    if (containerCL == null) {
      this.containerCL = (URLClassLoader) getClass().getClassLoader();
    } else {
      this.containerCL = containerCL;
    }
    if (apiCL == null) {
      this.apiCL = (URLClassLoader) this.containerCL.getParent();
    } else {
      this.apiCL = apiCL;
    }
    Utils.checkState(tempDir.isDirectory(), errorString("Temp directory does not exist: {}", tempDir));
    Utils.checkState(clusterManagerFile.isFile(),
      errorString("_cluster-manager does not exist: {}", clusterManagerFile));
    Utils.checkState(clusterManagerFile.canExecute(),
      errorString("_cluster-manager is not executable: {}", clusterManagerFile));
  }

  public ApplicationState submit(final PipelineConfiguration pipelineConfiguration,
    final StageLibraryTask stageLibrary, final File etcDir, final File resourcesDir, final File staticWebDir,
    final File bootstrapDir, final Map<String, String> environment, final Map<String, String> sourceInfo,
    final long timeout, RuleDefinitions ruleDefinitions, Acl acl) throws TimeoutException, IOException {

    return clusterProvider.startPipeline(systemProcessFactory, clusterManagerFile, tempDir, environment, sourceInfo,
      pipelineConfiguration, stageLibrary, etcDir, resourcesDir, staticWebDir, bootstrapDir, apiCL, containerCL,
      timeout, ruleDefinitions, acl);
  }

  public void kill(final ApplicationState applicationState, final PipelineConfiguration pipelineConfiguration)
    throws TimeoutException, IOException {
    clusterProvider.killPipeline(systemProcessFactory, clusterManagerFile, tempDir, applicationState.getId(),
      pipelineConfiguration);
  }

  public ClusterPipelineStatus getStatus(final ApplicationState applicationState,
    final PipelineConfiguration pipelineConfiguration) throws TimeoutException, IOException {
    return clusterProvider.getStatus(systemProcessFactory, clusterManagerFile, tempDir, applicationState.getId(),
      pipelineConfiguration);
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }
}
