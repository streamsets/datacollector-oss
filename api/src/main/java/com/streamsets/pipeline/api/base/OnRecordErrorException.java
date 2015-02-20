/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;

public class OnRecordErrorException extends StageException {

  public OnRecordErrorException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}
