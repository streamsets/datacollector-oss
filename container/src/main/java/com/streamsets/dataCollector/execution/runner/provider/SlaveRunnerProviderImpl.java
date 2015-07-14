/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner.provider;

import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.manager.RunnerProvider;
import com.streamsets.dataCollector.execution.runner.common.dagger.PipelineProviderModule;
import com.streamsets.dataCollector.execution.runner.slave.dagger.SlaveRunnerModule;
import com.streamsets.dataCollector.execution.runner.standalone.dagger.StandaloneRunnerInjectorModule;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import dagger.ObjectGraph;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class SlaveRunnerProviderImpl implements RunnerProvider {

  @Inject
  public SlaveRunnerProviderImpl() {
  }

  @Override
  public Runner createRunner( String user, String name, String rev, PipelineConfigBean pipelineConfigBean,
      ObjectGraph objectGraph) {
    List<Object> modules = new ArrayList<>();
    objectGraph = objectGraph.plus(StandaloneRunnerInjectorModule.class);
    modules.add(new SlaveRunnerModule(user, name, rev, objectGraph));
    modules.add(new PipelineProviderModule(name, rev));
    ObjectGraph plus =  objectGraph.plus(modules.toArray());
    return plus.get(Runner.class);
  }
}
