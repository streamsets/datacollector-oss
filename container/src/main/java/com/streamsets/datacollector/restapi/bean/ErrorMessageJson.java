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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorMessageJson {

  private final ErrorMessage errorMessage;

  @JsonCreator
  public ErrorMessageJson(
      @JsonProperty("errorCode") String errorCode,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("nonLocalized") String nonLocalized,
      @JsonProperty("localized") String localized,
      @JsonProperty("errorStackTrace") String errorStackTrace,
      @JsonProperty("antennaDoctorMessages") List<AntennaDoctorMessageJson> antennaDoctorMessages
  ) {
    this.errorMessage = new ErrorMessage(errorCode, nonLocalized, timestamp);
  }

  public ErrorMessageJson(ErrorMessage errorMessage) {
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

  public List<AntennaDoctorMessageJson> getAntennaDoctorMessages() {
    return BeanHelper.wrapAntennaDoctorMessages(errorMessage.getAntennaDoctorMessages());
  }

  @JsonIgnore
  public ErrorMessage getErrorMessage() {
    return errorMessage;
  }
}
