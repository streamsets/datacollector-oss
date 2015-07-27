/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store;

import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.ErrorCode;

public class PipelineStoreException extends PipelineException {


  public PipelineStoreException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}
