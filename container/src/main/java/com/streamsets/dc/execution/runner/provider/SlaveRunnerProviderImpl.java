/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.provider;

import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.manager.RunnerProvider;
import com.streamsets.dc.execution.runner.slave.dagger.SlaveRunnerModule;
import com.streamsets.dc.execution.runner.standalone.dagger.StandaloneRunnerInjectorModule;
import com.streamsets.pipeline.creation.PipelineConfigBean;
import dagger.ObjectGraph;

import javax.inject.Inject;

public class SlaveRunnerProviderImpl implements RunnerProvider {

  @Inject
  public SlaveRunnerProviderImpl() {
  }

  @Override
  public Runner createRunner( String user, String name, String rev, PipelineConfigBean pipelineConfigBean,
      ObjectGraph objectGraph) {
    objectGraph = objectGraph.plus(StandaloneRunnerInjectorModule.class);
    ObjectGraph plus =  objectGraph.plus(new SlaveRunnerModule(user, name, rev, objectGraph));
    return plus.get(Runner.class);
  }
}
