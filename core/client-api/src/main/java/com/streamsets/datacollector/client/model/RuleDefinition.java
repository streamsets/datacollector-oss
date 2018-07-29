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
public class RuleDefinition   {

  private String id = null;
  private String condition = null;
  private String alertText = null;
  private Boolean sendEmail = null;
  private Boolean enabled = null;
  private Boolean valid = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("id")
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("condition")
  public String getCondition() {
    return condition;
  }
  public void setCondition(String condition) {
    this.condition = condition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("alertText")
  public String getAlertText() {
    return alertText;
  }
  public void setAlertText(String alertText) {
    this.alertText = alertText;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("sendEmail")
  public Boolean getSendEmail() {
    return sendEmail;
  }
  public void setSendEmail(Boolean sendEmail) {
    this.sendEmail = sendEmail;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("enabled")
  public Boolean getEnabled() {
    return enabled;
  }
  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("valid")
  public Boolean getValid() {
    return valid;
  }
  public void setValid(Boolean valid) {
    this.valid = valid;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class RuleDefinition {\n");

    sb.append("    id: ").append(StringUtil.toIndentedString(id)).append("\n");
    sb.append("    condition: ").append(StringUtil.toIndentedString(condition)).append("\n");
    sb.append("    alertText: ").append(StringUtil.toIndentedString(alertText)).append("\n");
    sb.append("    sendEmail: ").append(StringUtil.toIndentedString(sendEmail)).append("\n");
    sb.append("    enabled: ").append(StringUtil.toIndentedString(enabled)).append("\n");
    sb.append("    valid: ").append(StringUtil.toIndentedString(valid)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
