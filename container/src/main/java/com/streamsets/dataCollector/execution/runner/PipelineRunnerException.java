/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.util.PipelineException;

public class PipelineRunnerException extends PipelineException {

  public PipelineRunnerException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}