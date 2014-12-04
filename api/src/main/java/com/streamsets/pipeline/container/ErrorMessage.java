/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.ErrorCode;

public class ErrorMessage implements LocalizableString {
  private static final Object[] NULL_ONE_ARG = {null};

  private final ErrorCode errorCode;
  private final LocalizableString localizableMessage;
  private final long timestamp;

  public ErrorMessage(ErrorCode errorCode, Object... params) {
    this(Utils.checkNotNull(errorCode, "errorCode").getClass().getName(), errorCode, params);
  }

  public ErrorMessage(String resourceBundle, ErrorCode errorCode, Object... params) {
    this.errorCode = Utils.checkNotNull(errorCode, "errorCode");
    params = (params != null) ? params : NULL_ONE_ARG;
    localizableMessage = new LocalizableMessage(errorCode.getClass().getClassLoader(), resourceBundle,
                                                errorCode.getCode(), errorCode.getMessage(), params);
    timestamp = System.currentTimeMillis();
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getNonLocalized() {
    return Utils.format("{} - {}", getErrorCode().getCode(), localizableMessage.getNonLocalized());
  }

  @Override
  public String getLocalized() {
    return Utils.format("{} - {}", getErrorCode().getCode(), localizableMessage.getLocalized());
  }

}
