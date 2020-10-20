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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;

// This class was originally generated, however it's now maintained manually
@ApiModel(description = "")
public class StageDefinitionJson   {

  public enum TypeEnum {
    SOURCE("SOURCE"),
    PROCESSOR("PROCESSOR"),
    EXECUTOR("EXECUTOR"),
    TARGET("TARGET");

    private final String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum ExecutionModesEnum {
    STANDALONE("STANDALONE"),
    CLUSTER_BATCH("CLUSTER_BATCH"),
    CLUSTER_YARN_STREAMING("CLUSTER_YARN_STREAMING"),
    CLUSTER_MESOS_STREAMING("CLUSTER_MESOS_STREAMING"),
    SLAVE("SLAVE"),
    EDGE("EDGE"),
    EMR_BATCH("EMR_BATCH"),
    BATCH("BATCH"),
    STREAMING("STREAMING"),
    ;

    private final String value;

    ExecutionModesEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private String name = null;
  private TypeEnum type = null;
  private String className = null;
  private String label = null;
  private String libraryLabel = null;
  private ConfigGroupDefinitionJson configGroupDefinition = null;
  private RawSourceDefinitionJson rawSourceDefinition = null;
  private Boolean errorStage = null;
  private Boolean statsAggregatorStage = null;
  private Boolean connectionVerifierStage = null;
  private Boolean pipelineLifecycleStage = null;
  private Boolean offsetCommitTrigger = null;
  private Boolean variableOutputStreams = null;
  private Integer outputStreams = null;
  private String outputStreamLabelProviderClass = null;
  private List<String> outputStreamLabels = new ArrayList<String>();
  private String outputStreamsDrivenByConfig = null;
  private List<ServiceDependencyDefinitionJson> services = null;
  private List<String> hideStage = null;
  private List<ExecutionModesEnum> executionModes = new ArrayList<ExecutionModesEnum>();
  private String description = null;
  private Boolean privateClassLoader = null;
  private String library = null;
  private List<ConfigDefinitionJson> configDefinitions = new ArrayList<ConfigDefinitionJson>();
  private String version = null;
  private String icon = null;
  private Boolean onRecordError = null;
  private Boolean preconditions = null;
  private Boolean resetOffset = null;
  private Boolean producingEvents = null;
  private String onlineHelpRefUrl = null;
  private Boolean sendsResponse = null;
  private Boolean beta = null;
  private Integer inputStreams = null;
  private String inputStreamLabelProviderClass = null;
  private List<String> inputStreamLabels = null;
  private List<String> eventDefs = new ArrayList<>();
  private Boolean bisectable = false;
  private String yamlUpgrader;

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
  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }
  public void setType(TypeEnum type) {
    this.type = type;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("className")
  public String getClassName() {
    return className;
  }
  public void setClassName(String className) {
    this.className = className;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }
  public void setLabel(String label) {
    this.label = label;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("libraryLabel")
  public String getLibraryLabel() {
    return libraryLabel;
  }
  public void setLibraryLabel(String libraryLabel) {
    this.libraryLabel = libraryLabel;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configGroupDefinition")
  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return configGroupDefinition;
  }
  public void setConfigGroupDefinition(ConfigGroupDefinitionJson configGroupDefinition) {
    this.configGroupDefinition = configGroupDefinition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rawSourceDefinition")
  public RawSourceDefinitionJson getRawSourceDefinition() {
    return rawSourceDefinition;
  }
  public void setRawSourceDefinition(RawSourceDefinitionJson rawSourceDefinition) {
    this.rawSourceDefinition = rawSourceDefinition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorStage")
  public Boolean getErrorStage() {
    return errorStage;
  }
  public void setErrorStage(Boolean errorStage) {
    this.errorStage = errorStage;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("statsAggregatorStage")
  public Boolean getStatsAggregatorStage() {
    return statsAggregatorStage;
  }
  public void setStatsAggregatorStage(Boolean statsAggregatorStage) {
    this.statsAggregatorStage = statsAggregatorStage;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("connectionVerifierStage")
  public Boolean getConnectionVerifierStage() {
    return connectionVerifierStage;
  }
  public void setConnectionVerifierStage(Boolean connectionVerifierStage) {
    this.connectionVerifierStage = connectionVerifierStage;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("pipelineLifecycleStage")
  public Boolean getPipelineLifecycleStage() {
    return pipelineLifecycleStage;
  }
  public void setPipelineLifecycleStage(Boolean pipelineLifecycleStage) {
    this.pipelineLifecycleStage = pipelineLifecycleStage;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("offsetCommitTrigger")
  public Boolean getOffsetCommitTrigger() {
    return offsetCommitTrigger;
  }
  public void setOffsetCommitTrigger(Boolean offsetCommitTrigger) {
    this.offsetCommitTrigger = offsetCommitTrigger;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("variableOutputStreams")
  public Boolean getVariableOutputStreams() {
    return variableOutputStreams;
  }
  public void setVariableOutputStreams(Boolean variableOutputStreams) {
    this.variableOutputStreams = variableOutputStreams;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreams")
  public Integer getOutputStreams() {
    return outputStreams;
  }
  public void setOutputStreams(Integer outputStreams) {
    this.outputStreams = outputStreams;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreamLabelProviderClass")
  public String getOutputStreamLabelProviderClass() {
    return outputStreamLabelProviderClass;
  }
  public void setOutputStreamLabelProviderClass(String outputStreamLabelProviderClass) {
    this.outputStreamLabelProviderClass = outputStreamLabelProviderClass;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreamLabels")
  public List<String> getOutputStreamLabels() {
    return outputStreamLabels;
  }
  public void setOutputStreamLabels(List<String> outputStreamLabels) {
    this.outputStreamLabels = outputStreamLabels;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreamsDrivenByConfig")
  public String getOutputStreamsDrivenByConfig() {
    return outputStreamsDrivenByConfig;
  }
  public void setOutputStreamsDrivenByConfig(String outputStreamsDrivenByConfig) {
    this.outputStreamsDrivenByConfig = outputStreamsDrivenByConfig;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("services")
  public List<ServiceDependencyDefinitionJson> getServices() {
    return services;
  }
  public void setServices(List<ServiceDependencyDefinitionJson> services) {
    this.services = services;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("hideStage")
  public List<String> getHideStage() {
    return hideStage;
  }
  public void setHideStage(List<String>hideStage ) {
    this.hideStage = hideStage;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("executionModes")
  public List<ExecutionModesEnum> getExecutionModes() {
    return executionModes;
  }
  public void setExecutionModes(List<ExecutionModesEnum> executionModes) {
    this.executionModes = executionModes;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }
  public void setDescription(String description) {
    this.description = description;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("privateClassLoader")
  public Boolean getPrivateClassLoader() {
    return privateClassLoader;
  }
  public void setPrivateClassLoader(Boolean privateClassLoader) {
    this.privateClassLoader = privateClassLoader;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("library")
  public String getLibrary() {
    return library;
  }
  public void setLibrary(String library) {
    this.library = library;
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


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }
  public void setVersion(String version) {
    this.version = version;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("icon")
  public String getIcon() {
    return icon;
  }
  public void setIcon(String icon) {
    this.icon = icon;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("onRecordError")
  public Boolean getOnRecordError() {
    return onRecordError;
  }
  public void setOnRecordError(Boolean onRecordError) {
    this.onRecordError = onRecordError;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("preconditions")
  public Boolean getPreconditions() {
    return preconditions;
  }
  public void setPreconditions(Boolean preconditions) {
    this.preconditions = preconditions;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("resetOffset")
  public Boolean getResetOffset() {
    return resetOffset;
  }
  public void setResetOffset(Boolean resetOffset) {
    this.resetOffset = resetOffset;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("producingEvents")
  public Boolean getProducingEvents() {
    return producingEvents;
  }
  public void setProducingEvents(Boolean producingEvents) {
    this.producingEvents = producingEvents;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("onlineHelpRefUrl")
  public String getOnlineHelpRefUrl() {
    return onlineHelpRefUrl;
  }
  public void setOnlineHelpRefUrl(String onlineHelpRefUrl) {
    this.onlineHelpRefUrl = onlineHelpRefUrl;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("sendsResponse")
  public Boolean getSendsResponse() {
    return sendsResponse;
  }
  public void setSendsResponse(Boolean sendsResponse) {
    this.sendsResponse = sendsResponse;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("beta")
  public Boolean getBeta() {
    return beta;
  }
  public void setBeta(Boolean beta) {
    this.beta = beta;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("inputStreams")
  public Integer getInputStreams() {
    return inputStreams;
  }
  public void setInputStreams(Integer inputStreams) {
    this.inputStreams = inputStreams;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("inputStreamLabelProviderClass")
  public String getInputStreamLabelProviderClass() {
    return inputStreamLabelProviderClass;
  }
  public void setInputStreamLabelProviderClass(String inputStreamLabelProviderClass) {
    this.inputStreamLabelProviderClass = inputStreamLabelProviderClass;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("inputStreamLabels")
  public List<String> getInputStreamLabels() {
    return inputStreamLabels;
  }
  public void setInputStreamLabels(List<String> inputStreamLabels) {
    this.inputStreamLabels = inputStreamLabels;
  }

  public List<String> getEventDefs() {
    return eventDefs;
  }
  public void setEventDefs(List<String> eventDefs) {
    this.eventDefs = eventDefs;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("bisectable")
  public Boolean isBisectable() {
    return bisectable;
  }

  public void setBisectable(Boolean bisectable) {
    this.bisectable = bisectable;
  }

  @ApiModelProperty(value = "")
  @JsonProperty("yamlUpgrader")
  public String getYamlUpgrader() {
    return yamlUpgrader;
  }

  public StageDefinitionJson setYamlUpgrader(String yamlUpgrader) {
    this.yamlUpgrader = yamlUpgrader;
    return this;
  }

}
