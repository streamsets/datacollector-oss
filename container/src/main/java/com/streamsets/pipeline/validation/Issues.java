/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.validation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.util.NullDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonDeserialize(using = NullDeserializer.Object.class)
public class Issues {
  private final List<Issue> pipeline;
  private final Map<String, List<StageIssue>> stages;

  public Issues() {
    pipeline = new ArrayList<Issue>();
    stages = new HashMap<String, List<StageIssue>>();
  }

  public void addP(Issue issue) {
    pipeline.add(issue);
  }

  public void add(StageIssue issue) {
    List<StageIssue> stageIssues = stages.get(issue.getInstanceName());
    if (stageIssues == null) {
      stageIssues = new ArrayList<StageIssue>();
      stages.put(issue.getInstanceName(), stageIssues);
    }
    stageIssues.add(issue);
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
    return pipeline.size() + stages.size();
  }

  public String toString() {
    return Utils.format("Issues[pipeline='{}' stage='{}']", pipeline.size(), stages.size());
  }

}
