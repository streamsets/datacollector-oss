/**
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
import com.streamsets.datacollector.client.model.IssueJson;
import java.util.*;


import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2015-09-11T14:51:29.367-07:00")
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
