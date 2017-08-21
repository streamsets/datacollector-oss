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

  public String getErrorStackTrace() {
    return errorMessage.getErrorStackTrace();
  }

  @JsonIgnore
  public com.streamsets.pipeline.api.impl.ErrorMessage getErrorMessage() {
    return errorMessage;
  }
}
