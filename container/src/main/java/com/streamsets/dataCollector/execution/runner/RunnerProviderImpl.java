/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.manager.RunnerProvider;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import dagger.ObjectGraph;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class RunnerProviderImpl implements RunnerProvider {

  @Inject
  public RunnerProviderImpl() {
  }

  @Override
  public Runner createRunner(String user, String name, String rev, PipelineConfigBean pipelineConfigBean,
                             ObjectGraph objectGraph) {
    List<Object> modules = new ArrayList<>();
    switch (pipelineConfigBean.executionMode) {
      case CLUSTER:
        modules.add(new ClusterRunnerModule());
        break;
      case STANDALONE:
        modules.add(new StandaloneRunnerModule(user, name, rev, objectGraph));
        modules.add(new AsyncRunnerModule());
        modules.add(new PipelineProviderModule(name, rev));
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid execution mode '{}'", pipelineConfigBean.executionMode));
    }
    ObjectGraph plus =  objectGraph.plus(modules.toArray());
    return plus.get(Runner.class);
  }
}
