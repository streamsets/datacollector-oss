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
package com.streamsets.datacollector.validation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.datacollector.util.NullDeserializer;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class Issues implements Serializable {
  private final List<Issue> all;
  private final List<Issue> pipeline;
  private final Map<String, List<Issue>> stages;

  public Issues() {
    all = new ArrayList<>();
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
    all.add(issue);
    String instance = issue.getInstanceName();
    if (instance == null) {
      pipeline.add(issue);
    } else {
      List<Issue> stage = stages.get(instance);
      if (stage == null) {
        stage = new ArrayList<>();
        stages.put(instance, stage);
      }
      stage.add(issue);
    }
  }

  public List<Issue> getIssues() {
    return all;
  }

  public List<Issue> getPipelineIssues() {
    return pipeline;
  }

  public Map<String, List<Issue>> getStageIssues() {
    return stages;
  }

  public boolean hasIssues() {
    return !all.isEmpty();
  }

  public int getIssueCount() {
    return all.size();
  }

  public String toString() {
    return Utils.format("Issues[pipeline='{}' stage='{}']", pipeline.size(), stages.size());
  }

}
