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
public class DefinitionsJson   {

  private List<PipelineDefinitionJson> pipeline = new ArrayList<>();
  private List<PipelineRulesDefinitionJson> pipelineRules = new ArrayList<>();
  private List<StageDefinitionJson> stages = new ArrayList<>();
  private List<ServiceDefinitionJson> services = new ArrayList<>();
  private Map<String, Object> rulesElMetadata = new HashMap<>();
  private Map<String, Object> elCatalog = new HashMap<>();
  private List<Object> runtimeConfigs = new ArrayList<>();
  private Map<String, String> stageIcons = new HashMap<>();

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
  @JsonProperty("pipelineRules")
  public List<PipelineRulesDefinitionJson> getPipelineRules() {
    return pipelineRules;
  }
  public void setPipelineRules(List<PipelineRulesDefinitionJson> pipelineRules) {
    this.pipelineRules = pipelineRules;
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
  @JsonProperty("services")
  public List<ServiceDefinitionJson> getServices() {
    return services;
  }
  public void setServices(List<ServiceDefinitionJson> services) {
    this.services = services;
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

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageIcons")
  public Map<String, String> getStageIcons() {
    return stageIcons;
  }
  public void setStageIcons(Map<String, String> stageIcons) {
    this.stageIcons = stageIcons;
  }


  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class DefinitionsJson {\n");

    sb.append("    pipeline: ").append(StringUtil.toIndentedString(pipeline)).append("\n");
    sb.append("    pipelineRules: ").append(StringUtil.toIndentedString(pipelineRules)).append("\n");
    sb.append("    stages: ").append(StringUtil.toIndentedString(stages)).append("\n");
    sb.append("    services: ").append(StringUtil.toIndentedString(services)).append("\n");
    sb.append("    rulesElMetadata: ").append(StringUtil.toIndentedString(rulesElMetadata)).append("\n");
    sb.append("    elCatalog: ").append(StringUtil.toIndentedString(elCatalog)).append("\n");
    sb.append("    runtimeConfigs: ").append(StringUtil.toIndentedString(runtimeConfigs)).append("\n");
    sb.append("    stageIcons: ").append(StringUtil.toIndentedString(stageIcons)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
