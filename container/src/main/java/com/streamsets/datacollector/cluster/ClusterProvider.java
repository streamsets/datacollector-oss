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
package com.streamsets.datacollector.cluster;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.SystemProcessFactory;
import com.streamsets.lib.security.acl.dto.Acl;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface ClusterProvider {

  void killPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                   String appId, PipelineConfiguration pipelineConfiguration) throws TimeoutException,
                                   IOException;

  ClusterPipelineStatus getStatus(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                   String appId, PipelineConfiguration pipelineConfiguration) throws TimeoutException, IOException;


  ApplicationState startPipeline(
      SystemProcessFactory systemProcessFactory,
      File sparkManager,
      File tempDir,
      Map<String, String> environment,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      StageLibraryTask stageLibrary,
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
  )
    throws TimeoutException, IOException;
}
