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
public class ConfigGroupDefinitionJson   {

  private Map<String, List<String>> classNameToGroupsMap = new HashMap<String, List<String>>();
  private List<Map<String, String>> groupNameToLabelMapList = new ArrayList<Map<String, String>>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("classNameToGroupsMap")
  public Map<String, List<String>> getClassNameToGroupsMap() {
    return classNameToGroupsMap;
  }
  public void setClassNameToGroupsMap(Map<String, List<String>> classNameToGroupsMap) {
    this.classNameToGroupsMap = classNameToGroupsMap;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("groupNameToLabelMapList")
  public List<Map<String, String>> getGroupNameToLabelMapList() {
    return groupNameToLabelMapList;
  }
  public void setGroupNameToLabelMapList(List<Map<String, String>> groupNameToLabelMapList) {
    this.groupNameToLabelMapList = groupNameToLabelMapList;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConfigGroupDefinitionJson {\n");

    sb.append("    classNameToGroupsMap: ").append(StringUtil.toIndentedString(classNameToGroupsMap)).append("\n");
    sb.append("    groupNameToLabelMapList: ").append(StringUtil.toIndentedString(groupNameToLabelMapList)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
