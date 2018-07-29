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
import java.util.List;


@ApiModel(description = "")
public class RawSourceDefinitionJson   {

  private String rawSourcePreviewerClass = null;
  private String mimeType = null;
  private List<ConfigDefinitionJson> configDefinitions = new ArrayList<ConfigDefinitionJson>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rawSourcePreviewerClass")
  public String getRawSourcePreviewerClass() {
    return rawSourcePreviewerClass;
  }
  public void setRawSourcePreviewerClass(String rawSourcePreviewerClass) {
    this.rawSourcePreviewerClass = rawSourcePreviewerClass;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("mimeType")
  public String getMimeType() {
    return mimeType;
  }
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configDefinitions")
  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return configDefinitions;
  }
  public void setConfigDefinitions(List<ConfigDefinitionJson> configDefinitions) {
    this.configDefinitions = configDefinitions;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class RawSourceDefinitionJson {\n");

    sb.append("    rawSourcePreviewerClass: ").append(StringUtil.toIndentedString(rawSourcePreviewerClass)).append("\n");
    sb.append("    mimeType: ").append(StringUtil.toIndentedString(mimeType)).append("\n");
    sb.append("    configDefinitions: ").append(StringUtil.toIndentedString(configDefinitions)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
