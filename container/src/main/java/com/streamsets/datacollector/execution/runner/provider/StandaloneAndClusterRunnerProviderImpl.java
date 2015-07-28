/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.provider;

import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.execution.runner.cluster.dagger.ClusterRunnerInjectorModule;
import com.streamsets.datacollector.execution.runner.cluster.dagger.ClusterRunnerModule;
import com.streamsets.datacollector.execution.runner.standalone.dagger.StandaloneRunnerInjectorModule;
import com.streamsets.datacollector.execution.runner.standalone.dagger.StandaloneRunnerModule;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;

import dagger.ObjectGraph;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StandaloneAndClusterRunnerProviderImpl implements RunnerProvider {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneAndClusterRunnerProviderImpl.class);

  @Inject
  public StandaloneAndClusterRunnerProviderImpl() {
  }

  @Override
  public Runner createRunner(String user, String name, String rev, ObjectGraph objectGraph,
                             ExecutionMode executionMode) {
    List<Object> modules = new ArrayList<>();
    LOG.info(Utils.format("Pipeline execution mode is: {} ", executionMode));
    switch (executionMode) {
      case CLUSTER:
        objectGraph = objectGraph.plus(ClusterRunnerInjectorModule.class);
        modules.add(new ClusterRunnerModule(user, name, rev, objectGraph));
        break;
      case STANDALONE:
        objectGraph = objectGraph.plus(StandaloneRunnerInjectorModule.class);
        modules.add(new StandaloneRunnerModule(user, name, rev, objectGraph));
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid execution mode '{}'", executionMode));
    }
    ObjectGraph plus =  objectGraph.plus(modules.toArray());
    return plus.get(Runner.class);
  }
}
