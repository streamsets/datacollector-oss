/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.store;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.util.PipelineException;

public class PipelineStoreException extends PipelineException {


  public PipelineStoreException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}
