/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.List;

public class PipelineRuntimeException extends PipelineException {
  private final Issues issues;

  @SuppressWarnings("unchecked")
  public PipelineRuntimeException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
    issues = null;
  }

  public PipelineRuntimeException(Issues issues) {
    super(ContainerError.CONTAINER_0165, issues.getIssues());
    this.issues = issues;
  }

  public Issues getIssues() {
    return issues;
  }

}
