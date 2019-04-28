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
package com.streamsets.datacollector.antennadoctor.engine.context;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;

public class AntennaDoctorContext {
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final Configuration configuration;
  private final StageLibraryTask stageLibraryTask;

  public AntennaDoctorContext(
      RuntimeInfo runtimeInfo,
      BuildInfo buildInfo,
      Configuration configuration,
      StageLibraryTask stageLibraryTask
  ) {
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
    this.configuration = configuration;
    this.stageLibraryTask = stageLibraryTask;
  }

  public RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  public BuildInfo getBuildInfo() {
    return buildInfo;
  }

  public StageLibraryTask getStageLibraryTask() {
    return stageLibraryTask;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public AntennaDoctorStageContext forStage(
      StageDefinition stageDefinition,
      StageConfiguration stageConfiguration,
      PipelineConfiguration pipelineConfiguration
  ) {
    return new AntennaDoctorStageContext(
        stageDefinition,
        stageConfiguration,
        pipelineConfiguration,
        runtimeInfo,
        buildInfo,
        configuration,
        stageLibraryTask
    );
  }
}
