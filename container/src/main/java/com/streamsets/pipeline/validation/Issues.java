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
  private final List<Issue> pipeline;
  private final Map<String, List<StageIssue>> stages;

  public Issues() {
    this(new ArrayList<StageIssue>());
  }

  public Issues(List<StageIssue> issues) {
    pipeline = new ArrayList<>();
    stages = new HashMap<>();
    for (StageIssue issue : issues) {
      add(issue);
    }
  }

  public void addP(Issue issue) {
    pipeline.add(issue);
  }

  public void add(StageIssue issue) {
    List<StageIssue> stageIssues = stages.get(issue.getInstanceName());
    if (stageIssues == null) {
      stageIssues = new ArrayList<>();
      stages.put(issue.getInstanceName(), stageIssues);
    }
    stageIssues.add(issue);
  }

  private void addAll(List<StageIssue> issues) {
    for (StageIssue issue : issues) {
      add(issue);
    }
  }

  public List<Issue> getIssues() {
    List<Issue> result = new ArrayList<>();
    result.addAll(pipeline);
    for (List<StageIssue> stageIssues : stages.values()) {
      result.addAll(stageIssues);
    }
    return pipeline;
  }

  public List<Issue> getPipelineIssues() {
    return pipeline;
  }

  public Map<String, List<StageIssue>> getStageIssues() {
    return stages;
  }

  public boolean hasIssues() {
    return !pipeline.isEmpty() || !stages.isEmpty();
  }

  public int getIssueCount() {
    int issueCount = pipeline.size();
    for(String key: stages.keySet()) {
      issueCount += stages.get(key).size();
    }
    return issueCount;
  }

  public String toString() {
    return Utils.format("Issues[pipeline='{}' stage='{}']", pipeline.size(), stages.size());
  }

}
