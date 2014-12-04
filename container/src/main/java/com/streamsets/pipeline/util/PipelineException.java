/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.container.ErrorMessage;

public class PipelineException extends Exception {

  private static Throwable getCause(Object... params) {
    Throwable throwable = null;
    if (params.length > 0 && params[params.length - 1] instanceof Throwable) {
      throwable = (Throwable) params[params.length - 1];
    }
    return throwable;
  }

  private final ErrorMessage errorMessage;

  // last parameter can be an exception cause
  public PipelineException(ErrorCode errorCode, Object... params) {
    super(getCause(params));
    errorMessage = new ErrorMessage(errorCode, params);
  }

  public ErrorCode getErrorCode() {
    return errorMessage.getId();
  }

  @Override
  public String getMessage() {
    return errorMessage.getNonLocalized();
  }

  @Override
  public String getLocalizedMessage() {
    return errorMessage.getLocalized();
  }

}

