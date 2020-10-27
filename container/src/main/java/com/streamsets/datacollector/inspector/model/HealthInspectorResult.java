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
import com.streamsets.datacollector.inspector.HealthInspector;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class HealthInspectorResult {

  /**
   * Info structure describes inspector that generated this result.
   */
  private final HealthInspectorsInfo inspectorInfo;
  public HealthInspectorsInfo getInspectorInfo() {
    return inspectorInfo;
  }

  /**
   * Performed checks and their result.
   */
  private final List<HealthInspectorEntry> entries;
  public List<HealthInspectorEntry> getEntries() {
    return entries;
  }

  public HealthInspectorResult(HealthInspector checker, List<HealthInspectorEntry> entries) {
    this.inspectorInfo = new HealthInspectorsInfo(checker);
    this.entries = Collections.unmodifiableList(entries);
  }

  @JsonCreator
  public HealthInspectorResult(
      @JsonProperty("inspectorInfo") HealthInspectorsInfo inspectorsInfo,
      @JsonProperty("entries") List<HealthInspectorEntry> entries
  ) {
    this.inspectorInfo = inspectorsInfo;
    this.entries = entries;
  }

  public static class Builder {
    private final HealthInspector checker;
    private final List<HealthInspectorEntry.Builder> entries;

    public Builder(HealthInspector checker) {
      this.checker = checker;
      this.entries = new LinkedList<>();
    }

    public HealthInspectorEntry.Builder addEntry(String name, HealthInspectorEntry.Severity severity) {
      HealthInspectorEntry.Builder builder = new HealthInspectorEntry.Builder(name, severity);
      entries.add(builder);
      return builder;
    }

    public HealthInspectorResult build() {
      return new HealthInspectorResult(
          checker,
          entries.stream().map(HealthInspectorEntry.Builder::build).collect(Collectors.toList())
      );
    }
  }
}
