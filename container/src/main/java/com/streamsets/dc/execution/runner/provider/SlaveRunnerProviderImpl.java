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
    ObjectGraph plus =  objectGraph.plus(modules.toArray());
    return plus.get(Runner.class);
  }
}
