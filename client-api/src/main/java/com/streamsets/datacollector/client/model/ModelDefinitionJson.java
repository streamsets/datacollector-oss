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
public class ModelDefinitionJson   {

  private List<String> labels = new ArrayList<String>();
  private String valuesProviderClass = null;
  private List<String> values = new ArrayList<String>();
  private List<ConfigDefinitionJson> configDefinitions = new ArrayList<ConfigDefinitionJson>();
  private String filteringConfig = null;

  public enum ModelTypeEnum {
    FIELD_SELECTOR_MULTI_VALUE("FIELD_SELECTOR_MULTI_VALUE"),
    FIELD_SELECTOR("FIELD_SELECTOR"),
    FIELD_VALUE_CHOOSER("FIELD_VALUE_CHOOSER"),
    VALUE_CHOOSER("VALUE_CHOOSER"),
    MULTI_VALUE_CHOOSER("MULTI_VALUE_CHOOSER"),
    PREDICATE("PREDICATE"),
    LIST_BEAN("LIST_BEAN"),
    ;

    private String value;

    ModelTypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private ModelTypeEnum modelType = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("labels")
  public List<String> getLabels() {
    return labels;
  }
  public void setLabels(List<String> labels) {
    this.labels = labels;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("valuesProviderClass")
  public String getValuesProviderClass() {
    return valuesProviderClass;
  }
  public void setValuesProviderClass(String valuesProviderClass) {
    this.valuesProviderClass = valuesProviderClass;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("values")
  public List<String> getValues() {
    return values;
  }
  public void setValues(List<String> values) {
    this.values = values;
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
  @JsonProperty("modelType")
  public ModelTypeEnum getModelType() {
    return modelType;
  }
  public void setModelType(ModelTypeEnum modelType) {
    this.modelType = modelType;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("filteringConfig")
  public String getFilteringConfig() {
    return filteringConfig;
  }
  public void setFilteringConfig(String filteringConfig) {
    this.filteringConfig = filteringConfig;
  }


  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class ModelDefinitionJson {\n");

    sb.append("    labels: ").append(StringUtil.toIndentedString(labels)).append("\n");
    sb.append("    valuesProviderClass: ").append(StringUtil.toIndentedString(valuesProviderClass)).append("\n");
    sb.append("    values: ").append(StringUtil.toIndentedString(values)).append("\n");
    sb.append("    configDefinitions: ").append(StringUtil.toIndentedString(configDefinitions)).append("\n");
    sb.append("    modelType: ").append(StringUtil.toIndentedString(modelType)).append("\n");
    sb.append("    filteringConfig: ").append(StringUtil.toIndentedString(filteringConfig)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
