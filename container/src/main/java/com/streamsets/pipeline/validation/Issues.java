/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.util.NullDeserializer;

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
