/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview;

import com.streamsets.dataCollector.execution.PreviewOutput;
import com.streamsets.dataCollector.execution.PreviewStatus;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.validation.Issues;

import java.util.List;

public class PreviewOutputImpl implements PreviewOutput {

  private final PreviewStatus previewStatus;
  private final Issues issues;
  private final List<List<StageOutput>> output;

  public PreviewOutputImpl(PreviewStatus previewStatus, Issues issues, List<List<StageOutput>> output) {
    this.previewStatus = previewStatus;
    this.issues = issues;
    this.output = output;
  }

  @Override
  public PreviewStatus getStatus() {
    return previewStatus;
  }

  @Override
  public Issues getIssues() {
    return issues;
  }

  @Override
  public List<List<StageOutput>> getOutput() {
    return output;
  }
}
