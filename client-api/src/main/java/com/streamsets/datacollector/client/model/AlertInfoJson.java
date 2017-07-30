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


@ApiModel(description = "")
public class AlertInfoJson   {

  private Gauge gauge = null;
  private String pipelineName = null;
  private RuleDefinition ruleDefinition = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("gauge")
  public Gauge getGauge() {
    return gauge;
  }
  public void setGauge(Gauge gauge) {
    this.gauge = gauge;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("pipelineName")
  public String getPipelineName() {
    return pipelineName;
  }
  public void setPipelineName(String pipelineName) {
    this.pipelineName = pipelineName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("ruleDefinition")
  public RuleDefinition getRuleDefinition() {
    return ruleDefinition;
  }
  public void setRuleDefinition(RuleDefinition ruleDefinition) {
    this.ruleDefinition = ruleDefinition;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class AlertInfoJson {\n");

    sb.append("    gauge: ").append(StringUtil.toIndentedString(gauge)).append("\n");
    sb.append("    pipelineName: ").append(StringUtil.toIndentedString(pipelineName)).append("\n");
    sb.append("    ruleDefinition: ").append(StringUtil.toIndentedString(ruleDefinition)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
