/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.util.PipelineException;
import com.streamsets.pipeline.validation.Issue;

import java.util.Collections;
import java.util.List;

public class PipelineRuntimeException extends PipelineException {
  private final List<Issue> issues;

  public PipelineRuntimeException(ErrorCode errorCode, List<Issue> issues) {
    super(errorCode, issues);
    this.issues = issues;
  }

  @SuppressWarnings("unchecked")
  public PipelineRuntimeException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
    issues = Collections.EMPTY_LIST;
  }

  public List<Issue> getIssues() {
    return issues;
  }

}
