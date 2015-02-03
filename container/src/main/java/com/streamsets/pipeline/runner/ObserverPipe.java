/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.validation.StageIssue;

import java.util.Collections;
import java.util.List;

public class ObserverPipe extends Pipe {
  private final Observer observer;

  public ObserverPipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes, Observer observer) {
    super(stage, inputLanes, outputLanes);
    this.observer = observer;
  }

  @Override
  public List<StageIssue> validateConfigs() {
    return Collections.emptyList();
  }

  @Override
  public void init() throws StageException {
  }

  @Override
  public void destroy() {
  }

  @Override
  public void process(PipeBatch pipeBatch) throws PipelineRuntimeException {
    if (observer != null && observer.isObserving(getStage().getInfo())) {
      observer.observe(this, pipeBatch.getPipeLanesSnapshot(getInputLanes()));
    }
    for (int i = 0; i < getInputLanes().size(); i++) {
      pipeBatch.moveLane(getInputLanes().get(i), getOutputLanes().get(i));
    }
  }

}
