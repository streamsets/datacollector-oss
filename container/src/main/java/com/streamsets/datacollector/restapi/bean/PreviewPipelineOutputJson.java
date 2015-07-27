/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class PreviewPipelineOutputJson {

  private final com.streamsets.datacollector.runner.preview.PreviewPipelineOutput previewPipelineOutput;

  public PreviewPipelineOutputJson(com.streamsets.datacollector.runner.preview.PreviewPipelineOutput previewPipelineOutput) {
    this.previewPipelineOutput = previewPipelineOutput;
  }

  public IssuesJson getIssues() {
    return new IssuesJson(previewPipelineOutput.getIssues());
  }

  public MetricRegistry getMetrics() {
    return previewPipelineOutput.getMetrics();
  }

  public List<List<StageOutputJson>> getBatchesOutput() {
    return BeanHelper.wrapStageOutputLists(previewPipelineOutput.getBatchesOutput());
  }

  public String getSourceOffset() {
    return previewPipelineOutput.getSourceOffset();
  }

  public String getNewSourceOffset() {
    return previewPipelineOutput.getNewSourceOffset();
  }

  @JsonIgnore
  public com.streamsets.datacollector.runner.preview.PreviewPipelineOutput getPreviewPipelineOutput() {
    return previewPipelineOutput;
  }
}