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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.HashMap;
import java.util.Map;


@ApiModel(description = "")
public class RuleIssueJson   {

  private String message = null;
  private Map<String, Object> additionalInfo = new HashMap<String, Object>();
  private String ruleId = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("message")
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("additionalInfo")
  public Map<String, Object> getAdditionalInfo() {
    return additionalInfo;
  }
  public void setAdditionalInfo(Map<String, Object> additionalInfo) {
    this.additionalInfo = additionalInfo;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("ruleId")
  public String getRuleId() {
    return ruleId;
  }
  public void setRuleId(String ruleId) {
    this.ruleId = ruleId;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class RuleIssueJson {\n");

    sb.append("    message: ").append(StringUtil.toIndentedString(message)).append("\n");
    sb.append("    additionalInfo: ").append(StringUtil.toIndentedString(additionalInfo)).append("\n");
    sb.append("    ruleId: ").append(StringUtil.toIndentedString(ruleId)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
