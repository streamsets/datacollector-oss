/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;

public class ToRecordException extends StageException {

  public ToRecordException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
  }

}
