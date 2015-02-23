/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;

public class ErrorMessage implements LocalizableString {
  private static final Object[] NULL_ONE_ARG = {null};

  private final String errorCode;
  private final LocalizableString localizableMessage;
  private final long timestamp;
  private final boolean preppendErrorCode;

  public ErrorMessage(final StageException ex) {
    errorCode = ex.getErrorCode().getCode();
    timestamp = System.currentTimeMillis();
    localizableMessage = new LocalizableString() {
      @Override
      public String getNonLocalized() {
        return ex.getMessage();
      }

      @Override
      public String getLocalized() {
        return ex.getLocalizedMessage();
      }
    };
    preppendErrorCode = false;
  }

  public ErrorMessage(String errorCode, final String nonLocalizedMsg, long timestamp) {
    this.errorCode = errorCode;
    this.localizableMessage = new LocalizableString() {
      @Override
      public String getNonLocalized() {
        return nonLocalizedMsg;
      }

      @Override
      public String getLocalized() {
        return nonLocalizedMsg;
      }
    };
    this.timestamp = timestamp;
    preppendErrorCode = true;
  }

  public ErrorMessage(ErrorCode errorCode, Object... params) {
    this(Utils.checkNotNull(errorCode, "errorCode").getClass().getName() + "-bundle", errorCode, params);
  }

  public ErrorMessage(String resourceBundle, ErrorCode errorCode, Object... params) {
    this.errorCode = Utils.checkNotNull(errorCode, "errorCode").getCode();
    params = (params != null) ? params : NULL_ONE_ARG;
    localizableMessage = new LocalizableMessage(errorCode.getClass().getClassLoader(), resourceBundle,
                                                errorCode.getCode(), errorCode.getMessage(), params);
    timestamp = System.currentTimeMillis();
    preppendErrorCode = true;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getNonLocalized() {
    return (preppendErrorCode) ? Utils.format("{} - {}", getErrorCode(), localizableMessage.getNonLocalized())
                               : localizableMessage.getNonLocalized();
  }

  @Override
  public String getLocalized() {
    return (preppendErrorCode) ? Utils.format("{} - {}", getErrorCode(), localizableMessage.getLocalized())
                               : localizableMessage.getLocalized();
  }

}
