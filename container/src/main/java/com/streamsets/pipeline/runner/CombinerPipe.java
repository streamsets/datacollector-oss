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

public class CombinerPipe extends Pipe<Pipe.Context> {

  public CombinerPipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    super(stage, inputLanes, outputLanes);
  }

  @Override
  public List<Issue> init(Pipe.Context pipeContext) {
    return Collections.emptyList();
  }

  @Override
  public void destroy() {
  }

  @Override
  public void process(PipeBatch pipeBatch) throws PipelineRuntimeException {
    pipeBatch.combineLanes(getInputLanes(), getOutputLanes().get(0));
  }

}
