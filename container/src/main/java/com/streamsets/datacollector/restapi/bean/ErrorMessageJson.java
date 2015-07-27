/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.pipeline.api.impl.Utils;

public class ErrorMessageJson {

  private final com.streamsets.pipeline.api.impl.ErrorMessage errorMessage;

  public ErrorMessageJson(com.streamsets.pipeline.api.impl.ErrorMessage errorMessage) {
    Utils.checkNotNull(errorMessage, "errorMessage");
    this.errorMessage = errorMessage;
  }

  public String getErrorCode() {
    return errorMessage.getErrorCode();
  }

  public long getTimestamp() {
    return errorMessage.getTimestamp();
  }

  public String getNonLocalized() {
    return errorMessage.getNonLocalized();
  }

  public String getLocalized() {
    return errorMessage.getLocalized();
  }

  @JsonIgnore
  public com.streamsets.pipeline.api.impl.ErrorMessage getErrorMessage() {
    return errorMessage;
  }
}
