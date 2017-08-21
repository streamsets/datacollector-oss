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
package com.streamsets.datacollector.execution.runner.cluster.dagger;

import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.cluster.SlaveCallbackManager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = {MetricsEventRunnable.class}, library = true, complete = false)
public class ClusterMetricEventRunnableModule {

  private final String name;
  private final String rev;

  public ClusterMetricEventRunnableModule(String name, String rev) {
    this.name = name;
    this.rev = rev;
  }

  @Provides @Singleton
  public MetricsEventRunnable provideMetricsEventRunnable(
      Configuration configuration,
      PipelineStateStore pipelineStateStore,
      EventListenerManager eventListenerManager,
      SlaveCallbackManager slaveCallbackManager,
      RuntimeInfo runtimeInfo
  ) {
    return new MetricsEventRunnable(
        name,
        rev,
        configuration,
        pipelineStateStore,
        null,
        eventListenerManager,
        null,
        slaveCallbackManager,
        runtimeInfo
    );
  }

}
