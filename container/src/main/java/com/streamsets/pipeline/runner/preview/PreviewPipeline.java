/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.validation.Issues;

import java.util.Collections;
import java.util.List;

public class PreviewPipeline {
  private final Pipeline pipeline;
  private final Issues issues;

  public PreviewPipeline(Pipeline pipeline, Issues issues) {
    this.issues = issues;
    this.pipeline = pipeline;
  }

  @SuppressWarnings("unchecked")
  public PreviewPipelineOutput run() throws StageException, PipelineRuntimeException{
    return run(Collections.EMPTY_LIST);
  }

  public PreviewPipelineOutput run(List<StageOutput> stageOutputsToOverride)
      throws StageException, PipelineRuntimeException{
    pipeline.init();
    try {
      pipeline.run(stageOutputsToOverride);
    } finally {
      pipeline.destroy();
    }
    return new PreviewPipelineOutput(issues, pipeline.getRunner());
  }

}
