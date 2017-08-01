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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.client.StringUtil;



import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
public class PreviewInfoJson   {

  private String previewerId = null;

public enum StatusEnum {
  CREATED("CREATED"),
  VALIDATING("VALIDATING"),
  VALID("VALID"),
  INVALID("INVALID"),
  VALIDATION_ERROR("VALIDATION_ERROR"),
  STARTING("STARTING"),
  START_ERROR("START_ERROR"),
  RUNNING("RUNNING"),
  RUN_ERROR("RUN_ERROR"),
  FINISHING("FINISHING"),
  FINISHED("FINISHED"),
  CANCELLING("CANCELLING"),
  CANCELLED("CANCELLED"),
  TIMING_OUT("TIMING_OUT"),
  TIMED_OUT("TIMED_OUT")
  ;

  private String value;

  StatusEnum(String value) {
    this.value = value;
  }

  @JsonIgnore
  public boolean isOneOf(StatusEnum ...types) {
    if(types == null) {
      return false;
    }

    for(StatusEnum t : types) {
      if(this == t) {
        return true;
      }
    }

    return false;
  }

  @Override
  public String toString() {
    return value;
  }
}

  private StatusEnum status = null;

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("previewerId")
  public String getPreviewerId() {
    return previewerId;
  }
  public void setPreviewerId(String previewerId) {
    this.previewerId = previewerId;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("status")
  public StatusEnum getStatus() {
    return status;
  }
  public void setStatus(StatusEnum status) {
    this.status = status;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class PreviewInfoJson {\n");

    sb.append("    previewerId: ").append(StringUtil.toIndentedString(previewerId)).append("\n");
    sb.append("    status: ").append(StringUtil.toIndentedString(status)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
