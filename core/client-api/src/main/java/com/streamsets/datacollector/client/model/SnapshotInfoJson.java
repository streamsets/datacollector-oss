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


import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


@ApiModel(description = "")
public class SnapshotInfoJson   {

  private String user = null;
  private String id = null;
  private String label = null;
  private String name = null;
  private String rev = null;
  private Long timeStamp = null;
  private Boolean inProgress = null;
  private Boolean failureSnapshot = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("user")
  public String getUser() {
    return user;
  }
  public void setUser(String user) {
    this.user = user;
  }


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
  @JsonProperty("label")
  public String getLabel() {
    return id;
  }
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("name")
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rev")
  public String getRev() {
    return rev;
  }
  public void setRev(String rev) {
    this.rev = rev;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("timeStamp")
  public Long getTimeStamp() {
    return timeStamp;
  }
  public void setTimeStamp(Long timeStamp) {
    this.timeStamp = timeStamp;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("inProgress")
  public Boolean getInProgress() {
    return inProgress;
  }
  public void setInProgress(Boolean inProgress) {
    this.inProgress = inProgress;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("failureSnapshot")
  public Boolean getFailureSnapshot() {
    return failureSnapshot;
  }
  public void setFailureSnapshot(Boolean failureSnapshot) {
    this.failureSnapshot = failureSnapshot;
  }


  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class SnapshotInfoJson {\n");

    sb.append("    user: ").append(StringUtil.toIndentedString(user)).append("\n");
    sb.append("    id: ").append(StringUtil.toIndentedString(id)).append("\n");
    sb.append("    label: ").append(StringUtil.toIndentedString(label)).append("\n");
    sb.append("    name: ").append(StringUtil.toIndentedString(name)).append("\n");
    sb.append("    rev: ").append(StringUtil.toIndentedString(rev)).append("\n");
    sb.append("    timeStamp: ").append(StringUtil.toIndentedString(timeStamp)).append("\n");
    sb.append("    inProgress: ").append(StringUtil.toIndentedString(inProgress)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
