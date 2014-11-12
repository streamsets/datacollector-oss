/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.container.Utils;

import java.util.List;

public class ModelDefinition {

  private final ModelType modelType;
  private final FieldModifierType fieldModifierType;
  private final String valuesProviderClass;
  private List<String> values;
  private List<String> labels;

  @JsonCreator
  public ModelDefinition(
    @JsonProperty("modelType") ModelType modelType,
    @JsonProperty("fieldModifierType") FieldModifierType fieldModifierType,
    @JsonProperty("valuesProviderClass")String valuesProviderClass,
    @JsonProperty("values") List<String> values,
    @JsonProperty("labels") List<String> labels) {
    this.modelType = modelType;
    this.fieldModifierType = fieldModifierType;
    this.valuesProviderClass = valuesProviderClass;
    this.values = values;
    this.labels = labels;
  }

  public ModelType getModelType() {
    return modelType;
  }

  public FieldModifierType getFieldModifierType() {
    return fieldModifierType;
  }

  public List<String> getValues() {
    return values;
  }

  public List<String> getLabels() {
    return labels;
  }

  public String getValuesProviderClass() {
    return valuesProviderClass;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  @Override
  public String toString() {
    return Utils.format("ModelDefinition[type='{}' valuesProviderClass='{}' values='{}']", getModelType(), getValues(),
                        getValuesProviderClass());
  }

}
