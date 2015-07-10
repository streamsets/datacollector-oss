/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.validation.Issue;

import java.util.Collections;
import java.util.List;

public class ObserverPipe extends Pipe<Pipe.Context> {
  private final Observer observer;

  public ObserverPipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes, Observer observer) {
    super(stage, inputLanes, outputLanes);
    this.observer = observer;
  }

  @Override
  public List<Issue> validateConfigs() {
    return Collections.emptyList();
  }

  @Override
  public void init(Context context) throws StageException {
  }

  @Override
  public void destroy() {
  }

  @Override
  public void process(PipeBatch pipeBatch) throws PipelineRuntimeException {
    if (observer != null && observer.isObserving(getInputLanes())) {
      observer.observe(this, pipeBatch.getLaneOutputRecords(getInputLanes()));
    }
    for (int i = 0; i < getInputLanes().size(); i++) {
      pipeBatch.moveLane(getInputLanes().get(i), getOutputLanes().get(i));
    }
  }

}
