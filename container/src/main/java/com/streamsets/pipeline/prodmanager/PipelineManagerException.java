/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.util.PipelineException;

public class PipelineManagerException extends PipelineException {

  public PipelineManagerException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}
