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
package com.streamsets.datacollector.validation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.util.NullDeserializer;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class Issues implements Serializable {
  private final List<Issue> pipeline;
  private final Map<String, Map<String, Issue>> stages;

  public Issues() {
    pipeline = new ArrayList<>();
    stages = new HashMap<>();
  }

  public Issues(List<Issue> issues) {
    this();
    addAll(issues);
  }

  public void addAll(List<Issue> issues) {
    for (Issue issue : issues) {
      add(issue);
    }
  }

  public void add(Issue issue) {
    String instance = issue.getInstanceName();
    if (instance == null) {
      pipeline.add(issue);
    } else {
      Map<String, Issue> stageIssues = stages.computeIfAbsent(instance, k -> new LinkedHashMap<>());
      String key = deDupKey(issue);

      // De-duplicate the same error codes by increasing their count
      if(stageIssues.containsKey(key)) {
        stageIssues.get(key).incCount();
      } else {
        stageIssues.put(key, issue);
      }
    }
  }

  private String deDupKey(Issue issue) {
    return "" + issue.getServiceName() + issue.getErrorCode() + issue.getConfigGroup() + issue.getConfigName();
  }

  public List<Issue> getIssues() {
    // Merge all components together and generate one list of all issues
    ImmutableList.Builder builder = ImmutableList.builder();
    builder.addAll(pipeline);
    getStageIssues().forEach((key, value) -> builder.addAll(value));
    return builder.build();
  }

  public List<Issue> getPipelineIssues() {
    return pipeline;
  }

  public Map<String, List<Issue>> getStageIssues() {
    // Long way of saying, discard all keys from inner map and merge all it's values into a list
    return stages.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> new ArrayList<>(entry.getValue().values())));
  }

  public boolean hasIssues() {
    return !pipeline.isEmpty() || !stages.isEmpty();
  }

  public int getIssueCount() {
    return pipeline.size() + stages.values().stream().mapToInt(m -> m.values().size()).sum();
  }

  public String toString() {
    return Utils.format("Issues[pipeline='{}' stage='{}']", pipeline.size(), stages.size());
  }

}
