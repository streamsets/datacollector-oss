/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.inspector.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.inspector.HealthCategory;

public class HeathCategoryInfo {
  /**
   * Human readable name of the inspector.
   */
  private final String name;
  public String getName() {
    return name;
  }

  /**
   * Full Class name of the inspector.
   */
  private final String fullClassName;
  public String getFullClassName() {
    return fullClassName;
  }

  /**
   * Short class name used in REST interface to identify the inspector.
   */
  private final String className;
  public String getClassName() {
    return className;
  }

  public HeathCategoryInfo(HealthCategory inspector) {
    this.name = inspector.getName();
    this.fullClassName = inspector.getClass().getName();
    this.className = inspector.getClass().getSimpleName();
  }

  @JsonCreator
  public HeathCategoryInfo(
      @JsonProperty("name") String name,
      @JsonProperty("fullClassName") String fullClassName,
      @JsonProperty("className") String className
  ) {
    this.name = name;
    this.fullClassName = fullClassName;
    this.className = className;
  }
}
