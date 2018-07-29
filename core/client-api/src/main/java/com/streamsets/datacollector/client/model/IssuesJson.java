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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ApiModel(description = "")
public class IssuesJson   {

  private List<IssueJson> pipelineIssues = new ArrayList<IssueJson>();
  private Map<String, List<IssueJson>> stageIssues = new HashMap<String, List<IssueJson>>();
  private Integer issueCount = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("pipelineIssues")
  public List<IssueJson> getPipelineIssues() {
    return pipelineIssues;
  }
  public void setPipelineIssues(List<IssueJson> pipelineIssues) {
    this.pipelineIssues = pipelineIssues;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageIssues")
  public Map<String, List<IssueJson>> getStageIssues() {
    return stageIssues;
  }
  public void setStageIssues(Map<String, List<IssueJson>> stageIssues) {
    this.stageIssues = stageIssues;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("issueCount")
  public Integer getIssueCount() {
    return issueCount;
  }
  public void setIssueCount(Integer issueCount) {
    this.issueCount = issueCount;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class IssuesJson {\n");

    sb.append("    pipelineIssues: ").append(StringUtil.toIndentedString(pipelineIssues)).append("\n");
    sb.append("    stageIssues: ").append(StringUtil.toIndentedString(stageIssues)).append("\n");
    sb.append("    issueCount: ").append(StringUtil.toIndentedString(issueCount)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
