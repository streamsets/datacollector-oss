/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issues;

import java.util.List;

public class PreviewPipelineOutput {
  private final Issues issues;
  private final MetricRegistry metrics;
  private final List<List<StageOutput>> batchesOutput;
  private String sourceOffset;
  private String newSourceOffset;

  public PreviewPipelineOutput(Issues issues, PipelineRunner runner) {
    this.issues = issues;
    this.metrics = runner.getMetrics();
    this.batchesOutput = runner.getBatchesOutput();
    this.sourceOffset = runner.getSourceOffset();
    this.newSourceOffset = runner.getNewSourceOffset();
  }

  public Issues getIssues() {
    return issues;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public List<List<StageOutput>> getBatchesOutput() {
    return batchesOutput;
  }

  public String getSourceOffset() {
    return sourceOffset;
  }

  public String getNewSourceOffset() {
    return newSourceOffset;
  }

}
