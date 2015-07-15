/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.provider;

import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.manager.RunnerProvider;
import com.streamsets.dc.execution.runner.cluster.dagger.ClusterRunnerInjectorModule;
import com.streamsets.dc.execution.runner.cluster.dagger.ClusterRunnerModule;
import com.streamsets.dc.execution.runner.standalone.dagger.StandaloneRunnerInjectorModule;
import com.streamsets.dc.execution.runner.standalone.dagger.StandaloneRunnerModule;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import dagger.ObjectGraph;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class StandaloneAndClusterRunnerProviderImpl implements RunnerProvider {

  @Inject
  public StandaloneAndClusterRunnerProviderImpl() {
  }

  @Override
  public Runner createRunner(String user, String name, String rev, PipelineConfigBean pipelineConfigBean,
                             ObjectGraph objectGraph) {
    List<Object> modules = new ArrayList<>();
    switch (pipelineConfigBean.executionMode) {
      case CLUSTER:
        objectGraph = objectGraph.plus(ClusterRunnerInjectorModule.class);
        modules.add(new ClusterRunnerModule(user, name, rev, objectGraph));
        break;
      case STANDALONE:
        objectGraph = objectGraph.plus(StandaloneRunnerInjectorModule.class);
        modules.add(new StandaloneRunnerModule(user, name, rev, objectGraph));
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid execution mode '{}'", pipelineConfigBean.executionMode));
    }
    ObjectGraph plus =  objectGraph.plus(modules.toArray());
    return plus.get(Runner.class);
  }
}
