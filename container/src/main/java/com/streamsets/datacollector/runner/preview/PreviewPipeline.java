/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner.preview;

import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.StageException;

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
    MetricsConfigurator.registerJmxMetrics(null);
    try {
      List<Issue> initIssues = pipeline.init();
      if (initIssues.isEmpty()) {
        pipeline.run(stageOutputsToOverride);
      } else {
        issues.addAll(initIssues);
        throw new PipelineRuntimeException(issues);
      }
    } finally {
      pipeline.destroy();
    }
    return new PreviewPipelineOutput(issues, pipeline.getRunner());
  }

  public List<Issue> validateConfigs() throws StageException {
    return pipeline.validateConfigs();
  }

  public void destroy() {
    pipeline.destroy();
  }

}
