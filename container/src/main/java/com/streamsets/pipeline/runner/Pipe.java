/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public abstract class Pipe {
  private final StageRuntime stage;
  private final List<String> inputLanes;
  private final List<String> outputLanes;

  public Pipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    this.stage = stage;
    this.inputLanes = inputLanes;
    this.outputLanes = outputLanes;
  }

  public StageRuntime getStage() {
    return stage;
  }

  public List<String> getInputLanes() {
    return inputLanes;
  }

  public List<String> getOutputLanes() {
    return outputLanes;
  }

  public abstract void init() throws StageException;

  public abstract void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException;

  public abstract void destroy();

  @Override
  public String toString() {
    return Utils.format("{}[instance='{}' input='{}' output='{}']", getClass().getSimpleName(),
                        getStage().getInfo().getInstanceName(), getInputLanes(), getOutputLanes());
  }
}
