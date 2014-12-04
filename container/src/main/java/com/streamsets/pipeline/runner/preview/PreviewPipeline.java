/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.validation.Issues;

public class PreviewPipeline {
  private final Pipeline pipeline;
  private final Issues issues;

  public PreviewPipeline(Pipeline pipeline, Issues issues) {
    this.issues = issues;
    this.pipeline = pipeline;
  }

  public PreviewPipelineOutput run() throws StageException, PipelineRuntimeException{
    pipeline.init();
    pipeline.run();
    pipeline.destroy();
    return new PreviewPipelineOutput(issues, pipeline.getRunner());
  }

}
