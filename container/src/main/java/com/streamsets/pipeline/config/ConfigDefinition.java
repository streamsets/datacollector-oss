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
import com.streamsets.pipeline.api.ConfigDef;

/**
 * Captures attributes related to individual configuration options
 */
public class ConfigDefinition {

  private final String name;
  private final ConfigDef.Type type;
  private final String label;
  private final String description;
  private final String defaultValue;
  private final boolean required;
  private final String group;

  @JsonCreator
  public ConfigDefinition(
      @JsonProperty("name") String name,
      @JsonProperty("type") ConfigDef.Type type,
      @JsonProperty("label") String label,
      @JsonProperty("description") String description,
      @JsonProperty("default") String defaultValue,
      @JsonProperty("required") boolean required,
      @JsonProperty("group") String group) {
    this.name = name;
    this.type = type;
    this.label = label;
    this.description = description;
    this.defaultValue = defaultValue;
    this.required = required;
    this.group = group;
  }

  public String getName() {
    return name;
  }

  public ConfigDef.Type getType() {
    return type;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public boolean isRequired() {
    return required;
  }

  public String getGroup() { return group; }

}
