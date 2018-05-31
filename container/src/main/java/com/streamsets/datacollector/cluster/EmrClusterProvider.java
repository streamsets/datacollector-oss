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
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class EmrClusterProvider extends BaseClusterProvider {

  public EmrClusterProvider(RuntimeInfo runtimeInfo, SecurityConfiguration securityConfiguration, Configuration conf) {
    super(runtimeInfo, securityConfiguration, conf);
  }

  @Override
  public void killPipeline(File tempDir, String appId, PipelineConfiguration pipelineConfiguration)
      throws TimeoutException, IOException {
  }

  @Override
  public ClusterPipelineStatus getStatus(File tempDir, String appId, PipelineConfiguration pipelineConfiguration)
      throws TimeoutException, IOException {
    return null;
  }

  protected ApplicationState startPipelineExecute(
      File outputDir,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean,
      long timeToWaitForFailure,
      File stagingDir,
      String clusterToken,
      File clusterBootstrapJar,
      File bootstrapJar, Set<String> jarsToShip,
      File libsTarGz,
      File resourcesTarGz,
      File etcTarGz,
      File sdcPropertiesFile,
      File log4jProperties,
      String mesosHostingJarDir,
      String mesosURL,
      String clusterBootstrapApiJar, List<Issue> errors
  ) throws IOException {
    return null;
  }

}
