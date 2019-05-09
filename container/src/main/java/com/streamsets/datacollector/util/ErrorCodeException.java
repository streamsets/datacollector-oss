/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;

public abstract class ErrorCodeException extends Exception {
  private final transient Object[] params;

  private static Throwable getCause(Object... params) {
    Throwable throwable = null;
    if (params.length > 0 && params[params.length - 1] instanceof Throwable) {
      throwable = (Throwable) params[params.length - 1];
    }
    return throwable;
  }

  private final ErrorCode errorCode;
  private final ErrorMessage errorMessage;

  // last parameter can be an exception cause
  public ErrorCodeException(ErrorCode errorCode, Object... params) {
    this.params = params;
    this.errorCode = errorCode;
    errorMessage = new ErrorMessage(errorCode, params);

    Throwable cause = getCause(params);
    if(cause != null) {
      initCause(cause);
    }
  }

  public ErrorMessage getErrorMessage() {
    return errorMessage;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  @Override
  public String getMessage() {
    return errorMessage.getNonLocalized();
  }

  @Override
  public String getLocalizedMessage() {
    return errorMessage.getLocalized();
  }

  public Object[] getParams() {
    return params;
  }
}
