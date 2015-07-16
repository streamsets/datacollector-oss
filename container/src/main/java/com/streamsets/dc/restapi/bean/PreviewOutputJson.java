/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.dc.execution.PreviewOutput;
import com.streamsets.dc.execution.PreviewStatus;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.IssuesJson;
import com.streamsets.pipeline.restapi.bean.StageOutputJson;

import java.util.List;

public class PreviewOutputJson {

  private final PreviewOutput previewOutput;

  public PreviewOutputJson(PreviewOutput previewOutput) {
    this.previewOutput = previewOutput;
  }

  public PreviewStatus getStatus() {
    return previewOutput.getStatus();
  }

  public IssuesJson getIssues() {
    return BeanHelper.wrapIssues(previewOutput.getIssues());
  }

  public List<List<StageOutputJson>> getBatchesOutput() {
    return BeanHelper.wrapStageOutputLists(previewOutput.getOutput());
  }

  @JsonIgnore
  public PreviewOutput getPreviewOutput() {
    return previewOutput;
  }
}