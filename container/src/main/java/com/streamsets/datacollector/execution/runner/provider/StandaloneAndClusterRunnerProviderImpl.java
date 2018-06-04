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
package com.streamsets.datacollector.execution.runner.provider;

import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.execution.runner.cluster.dagger.ClusterRunnerInjectorModule;
import com.streamsets.datacollector.execution.runner.cluster.dagger.ClusterRunnerModule;
import com.streamsets.datacollector.execution.runner.edge.dagger.EdgeRunnerInjectorModule;
import com.streamsets.datacollector.execution.runner.edge.dagger.EdgeRunnerModule;
import com.streamsets.datacollector.execution.runner.standalone.dagger.StandaloneRunnerInjectorModule;
import com.streamsets.datacollector.execution.runner.standalone.dagger.StandaloneRunnerModule;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import dagger.ObjectGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class StandaloneAndClusterRunnerProviderImpl implements RunnerProvider {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneAndClusterRunnerProviderImpl.class);

  @Inject
  public StandaloneAndClusterRunnerProviderImpl() {
  }

  @Override
  public Runner  createRunner(String name, String rev, ObjectGraph objectGraph, ExecutionMode executionMode) {
    List<Object> modules = new ArrayList<>();
    LOG.info(Utils.format("Pipeline execution mode is: {} ", executionMode));
    switch (executionMode) {
      case CLUSTER:
      case CLUSTER_BATCH:
      case CLUSTER_YARN_STREAMING:
      case CLUSTER_MESOS_STREAMING:
      case EMR_BATCH:
        objectGraph = objectGraph.plus(ClusterRunnerInjectorModule.class);
        modules.add(new ClusterRunnerModule(name, rev, objectGraph));
        break;
      case STANDALONE:
        objectGraph = objectGraph.plus(StandaloneRunnerInjectorModule.class);
        modules.add(new StandaloneRunnerModule(name, rev, objectGraph));
        break;
      case EDGE:
        objectGraph = objectGraph.plus(EdgeRunnerInjectorModule.class);
        modules.add(new EdgeRunnerModule(name, rev, objectGraph));
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid execution mode '{}'", executionMode));
    }
    ObjectGraph plus =  objectGraph.plus(modules.toArray());
    return plus.get(Runner.class);
  }
}
