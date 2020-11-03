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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class HealthCategoryResult {

  /**
   * Info structure describes category that generated this result.
   */
  private final HeathCategoryInfo categoryInfo;
  public HeathCategoryInfo getCategoryInfo() {
    return categoryInfo;
  }

  /**
   * Performed checks and their result.
   */
  private final List<HealthCheck> healthChecks;
  public List<HealthCheck> getHealthChecks() {
    return healthChecks;
  }

  public HealthCategoryResult(HealthCategory category, List<HealthCheck> healthChecks) {
    this.categoryInfo = new HeathCategoryInfo(category);
    this.healthChecks = Collections.unmodifiableList(healthChecks);
  }

  @JsonCreator
  public HealthCategoryResult(
      @JsonProperty("categoryInfo") HeathCategoryInfo categoryInfo,
      @JsonProperty("healthChecks") List<HealthCheck> healthChecks
  ) {
    this.categoryInfo = categoryInfo;
    this.healthChecks = healthChecks;
  }

  public static class Builder {
    private final HealthCategory category;
    private final List<HealthCheck.Builder> healthChecks;

    public Builder(HealthCategory category) {
      this.category = category;
      this.healthChecks = new LinkedList<>();
    }

    public HealthCheck.Builder addHealthCheck(String name, HealthCheck.Severity severity) {
      HealthCheck.Builder builder = new HealthCheck.Builder(name, severity);
      healthChecks.add(builder);
      return builder;
    }

    public HealthCategoryResult build() {
      return new HealthCategoryResult(
          category,
          healthChecks.stream().map(HealthCheck.Builder::build).collect(Collectors.toList())
      );
    }
  }
}
