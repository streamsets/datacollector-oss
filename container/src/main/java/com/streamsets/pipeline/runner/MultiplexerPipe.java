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

public class MultiplexerPipe extends Pipe {

  public MultiplexerPipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    super(stage, inputLanes, outputLanes);
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
    for (int i = 0; i < getInputLanes().size(); i++) {
      String inputStageLane = getStage().getConfiguration().getOutputLanes().get(i);
      String inputPipeLane = getInputLanes().get(i);
      List<String> outputLanes = LaneResolver.getMatchingOutputLanes(inputStageLane, getOutputLanes());
      if (outputLanes.size() == 1) {
        pipeBatch.moveLane(inputPipeLane, outputLanes.get(0));
      } else {
        pipeBatch.moveLaneCopying(inputPipeLane, outputLanes);
      }
    }
  }

}
