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

import java.util.List;

public class HealthReport {

  /**
   * Date when this report was generated.
   */
  private final String reportTime;
  public String getReportTime() {
    return reportTime;
  }

  /**
   * Number of milliseconds that this report took to generate.
   */
  private final long elapsedTime;
  public long getElapsedTime() {
    return elapsedTime;
  }

  /**
   * Individual results from the checks that were run.
   */
  private final List<HealthCategoryResult> categories;
  public List<HealthCategoryResult> getCategories() {
    return categories;
  }

  @JsonCreator
  public HealthReport(
      @JsonProperty("reportTime") String reportTime,
      @JsonProperty("elapsedTime") long elapsedTime,
      @JsonProperty("results") List<HealthCategoryResult> categories
  ) {
    this.reportTime = reportTime;
    this.elapsedTime = elapsedTime;
    this.categories = categories;
  }

}
