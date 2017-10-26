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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@ApiModel(description = "")
public class ConfigDefinitionJson   {

  private String name = null;

  public enum TypeEnum {
    BOOLEAN("BOOLEAN"),
    NUMBER("NUMBER"),
    STRING("STRING"),
    LIST("LIST"),
    MAP("MAP"),
    MODEL("MODEL"),
    CHARACTER("CHARACTER"),
    TEXT("TEXT"),
    CREDENTIAL("CREDENTIAL"),
    ;

    private String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private TypeEnum type = null;
  private Object defaultValue = null;
  private String label = null;
  private String mode = null;
  private Boolean required = null;
  private List<String> elDefs = new ArrayList<String>();
  private List<String> elFunctionDefinitionsIdx = new ArrayList<String>();
  private List<String> elConstantDefinitionsIdx = new ArrayList<String>();
  private ModelDefinitionJson model = null;
  private Integer lines = null;
  private Integer displayPosition = null;
  private Map<String, List<Object>> dependsOnMap = new HashMap<String, List<Object>>();
  private String description = null;
  private String dependsOn = null;
  private List<Object> triggeredByValues = new ArrayList<Object>();
  private BigInteger min;
  private String group = null;

public enum EvaluationEnum {
  IMPLICIT("IMPLICIT"),
  EXPLICIT("EXPLICIT");

  private String value;

  EvaluationEnum(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}

  private EvaluationEnum evaluation = null;
  private BigInteger max;
  private String fieldName = null;


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
  @JsonProperty("defaultValue")
  public Object getDefaultValue() {
    return defaultValue;
  }
  public void setDefaultValue(Object defaultValue) {
    this.defaultValue = defaultValue;
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
  @JsonProperty("mode")
  public String getMode() {
    return mode;
  }
  public void setMode(String mode) {
    this.mode = mode;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("required")
  public Boolean getRequired() {
    return required;
  }
  public void setRequired(Boolean required) {
    this.required = required;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("elDefs")
  public List<String> getElDefs() {
    return elDefs;
  }
  public void setElDefs(List<String> elDefs) {
    this.elDefs = elDefs;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("elFunctionDefinitionsIdx")
  public List<String> getElFunctionDefinitionsIdx() {
    return elFunctionDefinitionsIdx;
  }
  public void setElFunctionDefinitionsIdx(List<String> elFunctionDefinitionsIdx) {
    this.elFunctionDefinitionsIdx = elFunctionDefinitionsIdx;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("elConstantDefinitionsIdx")
  public List<String> getElConstantDefinitionsIdx() {
    return elConstantDefinitionsIdx;
  }
  public void setElConstantDefinitionsIdx(List<String> elConstantDefinitionsIdx) {
    this.elConstantDefinitionsIdx = elConstantDefinitionsIdx;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("model")
  public ModelDefinitionJson getModel() {
    return model;
  }
  public void setModel(ModelDefinitionJson model) {
    this.model = model;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("lines")
  public Integer getLines() {
    return lines;
  }
  public void setLines(Integer lines) {
    this.lines = lines;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("displayPosition")
  public Integer getDisplayPosition() {
    return displayPosition;
  }
  public void setDisplayPosition(Integer displayPosition) {
    this.displayPosition = displayPosition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("dependsOnMap")
  public Map<String, List<Object>> getDependsOnMap() {
    return dependsOnMap;
  }
  public void setDependsOnMap(Map<String, List<Object>> dependsOnMap) {
    this.dependsOnMap = dependsOnMap;
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
  @JsonProperty("dependsOn")
  public String getDependsOn() {
    return dependsOn;
  }
  public void setDependsOn(String dependsOn) {
    this.dependsOn = dependsOn;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("triggeredByValues")
  public List<Object> getTriggeredByValues() {
    return triggeredByValues;
  }
  public void setTriggeredByValues(List<Object> triggeredByValues) {
    this.triggeredByValues = triggeredByValues;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("min")
  public BigInteger getMin() {
    return min;
  }
  public void setMin(BigInteger min) {
    this.min = min;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("group")
  public String getGroup() {
    return group;
  }
  public void setGroup(String group) {
    this.group = group;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("evaluation")
  public EvaluationEnum getEvaluation() {
    return evaluation;
  }
  public void setEvaluation(EvaluationEnum evaluation) {
    this.evaluation = evaluation;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("max")
  public BigInteger getMax() {
    return max;
  }
  public void setMax(BigInteger max) {
    this.max = max;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("fieldName")
  public String getFieldName() {
    return fieldName;
  }
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConfigDefinitionJson {\n");

    sb.append("    name: ").append(StringUtil.toIndentedString(name)).append("\n");
    sb.append("    type: ").append(StringUtil.toIndentedString(type)).append("\n");
    sb.append("    defaultValue: ").append(StringUtil.toIndentedString(defaultValue)).append("\n");
    sb.append("    label: ").append(StringUtil.toIndentedString(label)).append("\n");
    sb.append("    mode: ").append(StringUtil.toIndentedString(mode)).append("\n");
    sb.append("    required: ").append(StringUtil.toIndentedString(required)).append("\n");
    sb.append("    elDefs: ").append(StringUtil.toIndentedString(elDefs)).append("\n");
    sb.append("    elFunctionDefinitionsIdx: ").append(StringUtil.toIndentedString(elFunctionDefinitionsIdx)).append("\n");
    sb.append("    elConstantDefinitionsIdx: ").append(StringUtil.toIndentedString(elConstantDefinitionsIdx)).append("\n");
    sb.append("    model: ").append(StringUtil.toIndentedString(model)).append("\n");
    sb.append("    lines: ").append(StringUtil.toIndentedString(lines)).append("\n");
    sb.append("    displayPosition: ").append(StringUtil.toIndentedString(displayPosition)).append("\n");
    sb.append("    dependsOnMap: ").append(StringUtil.toIndentedString(dependsOnMap)).append("\n");
    sb.append("    description: ").append(StringUtil.toIndentedString(description)).append("\n");
    sb.append("    dependsOn: ").append(StringUtil.toIndentedString(dependsOn)).append("\n");
    sb.append("    triggeredByValues: ").append(StringUtil.toIndentedString(triggeredByValues)).append("\n");
    sb.append("    min: ").append(StringUtil.toIndentedString(min)).append("\n");
    sb.append("    group: ").append(StringUtil.toIndentedString(group)).append("\n");
    sb.append("    evaluation: ").append(StringUtil.toIndentedString(evaluation)).append("\n");
    sb.append("    max: ").append(StringUtil.toIndentedString(max)).append("\n");
    sb.append("    fieldName: ").append(StringUtil.toIndentedString(fieldName)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
