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
package com.streamsets.datacollector.client.model;

import com.streamsets.datacollector.client.StringUtil;



import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
public class ErrorMessageJson   {

  private String errorCode = null;
  private String nonLocalized = null;
  private String localized = null;
  private Long timestamp = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorCode")
  public String getErrorCode() {
    return errorCode;
  }
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("nonLocalized")
  public String getNonLocalized() {
    return nonLocalized;
  }
  public void setNonLocalized(String nonLocalized) {
    this.nonLocalized = nonLocalized;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("localized")
  public String getLocalized() {
    return localized;
  }
  public void setLocalized(String localized) {
    this.localized = localized;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("timestamp")
  public Long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class ErrorMessageJson {\n");

    sb.append("    errorCode: ").append(StringUtil.toIndentedString(errorCode)).append("\n");
    sb.append("    nonLocalized: ").append(StringUtil.toIndentedString(nonLocalized)).append("\n");
    sb.append("    localized: ").append(StringUtil.toIndentedString(localized)).append("\n");
    sb.append("    timestamp: ").append(StringUtil.toIndentedString(timestamp)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
