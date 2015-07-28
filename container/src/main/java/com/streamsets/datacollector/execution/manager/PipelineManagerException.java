/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.datacollector.execution.manager;

import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.ErrorCode;

public class PipelineManagerException extends PipelineException {

  public PipelineManagerException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }
}
