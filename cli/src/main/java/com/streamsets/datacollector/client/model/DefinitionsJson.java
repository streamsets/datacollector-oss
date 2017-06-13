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
import com.streamsets.datacollector.client.model.StageDefinitionJson;
import com.streamsets.datacollector.client.model.PipelineDefinitionJson;
import java.util.*;


import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2015-09-11T14:51:29.367-07:00")
public class DefinitionsJson   {

  private List<PipelineDefinitionJson> pipeline = new ArrayList<PipelineDefinitionJson>();
  private List<StageDefinitionJson> stages = new ArrayList<StageDefinitionJson>();
  private Map<String, Object> rulesElMetadata = new HashMap<String, Object>();
  private Map<String, Object> elCatalog = new HashMap<String, Object>();
  private List<Object> runtimeConfigs = new ArrayList<Object>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("pipeline")
  public List<PipelineDefinitionJson> getPipeline() {
    return pipeline;
  }
  public void setPipeline(List<PipelineDefinitionJson> pipeline) {
    this.pipeline = pipeline;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stages")
  public List<StageDefinitionJson> getStages() {
    return stages;
  }
  public void setStages(List<StageDefinitionJson> stages) {
    this.stages = stages;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rulesElMetadata")
  public Map<String, Object> getRulesElMetadata() {
    return rulesElMetadata;
  }
  public void setRulesElMetadata(Map<String, Object> rulesElMetadata) {
    this.rulesElMetadata = rulesElMetadata;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("elCatalog")
  public Map<String, Object> getElCatalog() {
    return elCatalog;
  }
  public void setElCatalog(Map<String, Object> elCatalog) {
    this.elCatalog = elCatalog;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("runtimeConfigs")
  public List<Object> getRuntimeConfigs() {
    return runtimeConfigs;
  }
  public void setRuntimeConfigs(List<Object> runtimeConfigs) {
    this.runtimeConfigs = runtimeConfigs;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class DefinitionsJson {\n");

    sb.append("    pipeline: ").append(StringUtil.toIndentedString(pipeline)).append("\n");
    sb.append("    stages: ").append(StringUtil.toIndentedString(stages)).append("\n");
    sb.append("    rulesElMetadata: ").append(StringUtil.toIndentedString(rulesElMetadata)).append("\n");
    sb.append("    elCatalog: ").append(StringUtil.toIndentedString(elCatalog)).append("\n");
    sb.append("    runtimeConfigs: ").append(StringUtil.toIndentedString(runtimeConfigs)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
