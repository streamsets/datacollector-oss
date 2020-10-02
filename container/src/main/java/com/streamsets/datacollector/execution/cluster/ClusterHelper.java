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
import com.streamsets.datacollector.aster.AsterUtil;
import com.streamsets.datacollector.cluster.ApplicationState;
import com.streamsets.datacollector.cluster.ClusterPipelineStatus;
import com.streamsets.datacollector.cluster.ClusterProvider;
import com.streamsets.datacollector.cluster.ClusterProviderSelector;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.InterceptorCreatorContextBuilder;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.SystemProcessFactory;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.List;
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
  private URLClassLoader apiCL;
  private URLClassLoader containerCL;
  private URLClassLoader asterClientCL;
  private Configuration configuration;

  public ClusterHelper(
      RuntimeInfo runtimeInfo,
      SecurityConfiguration securityConfiguration,
      File tempDir,
      Configuration conf,
      StageLibraryTask stageLibraryTask
  ) {
    this(
        runtimeInfo,
        new SystemProcessFactory(),
        new ClusterProviderSelector(runtimeInfo, securityConfiguration, conf, stageLibraryTask),
        tempDir,
        null,
        null,
        null,
        securityConfiguration
    );
  }

  @VisibleForTesting
  public ClusterHelper(
      RuntimeInfo runtimeInfo,
      SystemProcessFactory systemProcessFactory,
      ClusterProvider clusterProvider,
      File tempDir,
      URLClassLoader apiCL,
      URLClassLoader containerCL,
      URLClassLoader asterClientCL,
      SecurityConfiguration securityConfiguration
  ) {
    this.systemProcessFactory = systemProcessFactory;
    this.clusterProvider = clusterProvider;
    this.tempDir = tempDir;
    // JDK 11 issue - https://issues.streamsets.com/browse/SDC-15791
    if (containerCL == null && getClass().getClassLoader() instanceof URLClassLoader) {
      this.containerCL = (URLClassLoader) getClass().getClassLoader();
    } else {
      this.containerCL = containerCL;
    }
    if (apiCL == null && getClass().getClassLoader() instanceof URLClassLoader) {
      this.apiCL = (URLClassLoader) this.containerCL.getParent();
    } else {
      this.apiCL = apiCL;
    }
    if (asterClientCL == null) {
      this.asterClientCL = SDCClassLoader.getAsterClassLoader(AsterUtil.getAsterJars(runtimeInfo), this.containerCL);
    } else {
      this.asterClientCL = asterClientCL;
    }
    Utils.checkState(tempDir.isDirectory(), errorString("Temp directory does not exist: {}", tempDir));
  }

  public ApplicationState submit(
      final PipelineConfiguration pipelineConfiguration,
      final PipelineConfigBean pipelineConfigBean,
      final StageLibraryTask stageLibrary,
      CredentialStoresTask credentialStoresTask,
      final File etcDir,
      final File resourcesDir,
      final File staticWebDir,
      final File bootstrapDir,
      final Map<String, String> sourceInfo,
      final long timeout,
      RuleDefinitions ruleDefinitions,
      Acl acl,
      InterceptorCreatorContextBuilder interceptorCreatorContextBuilder,
      List<String> blobStoreResources,
      String user
  ) throws TimeoutException, IOException, StageException {

    return clusterProvider.startPipeline(
        tempDir,
        sourceInfo,
        pipelineConfiguration,
        pipelineConfigBean,
        stageLibrary,
        credentialStoresTask,
        etcDir, resourcesDir,
        staticWebDir,
        bootstrapDir,
        apiCL,
        containerCL,
        asterClientCL,
        timeout,
        ruleDefinitions,
        acl,
        interceptorCreatorContextBuilder,
        blobStoreResources,
        user
    );
  }

  public void kill(
      final ApplicationState applicationState,
      final PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  )
      throws TimeoutException, IOException, StageException {
    clusterProvider.killPipeline(tempDir, applicationState, pipelineConfiguration, pipelineConfigBean);
  }

  public void cleanUp(
      final ApplicationState applicationState,
      final PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  )
      throws IOException, StageException {
    this.asterClientCL.close();
    clusterProvider.cleanUp(applicationState, pipelineConfiguration, pipelineConfigBean);
  }

  public ClusterPipelineStatus getStatus(
      final ApplicationState applicationState,
      final PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  ) throws TimeoutException, IOException, StageException {
    return clusterProvider.getStatus(tempDir, applicationState, pipelineConfiguration, pipelineConfigBean);
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }
}
