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
package com.streamsets.pipeline.validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.streamsets.pipeline.util.NullDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

@JsonDeserialize(using = NullDeserializer.class)
public class Issues implements Issue.ResourceBundleProvider {
  private static final String PIPELINE_CONTAINER_BUNDLE = "pipeline-container-bundle";

  private final List<Issue> pipeline;
  private final Map<String, List<StageIssue>> stages;
  private ResourceBundle resourceBundle;
  private Locale locale;

  public Issues() {
    pipeline = new ArrayList<Issue>();
    stages = new HashMap<String, List<StageIssue>>();
  }

  public void addP(Issue issue) {
    issue.setResourceBundleProvider(this);
    pipeline.add(issue);
  }

  public void add(StageIssue issue) {
    issue.setResourceBundleProvider(this);
    List<StageIssue> stageIssues = stages.get(issue.getInstanceName());
    if (stageIssues == null) {
      stageIssues = new ArrayList<StageIssue>();
      stages.put(issue.getInstanceName(), stageIssues);
    }
    stageIssues.add(issue);
  }

  @Override
  @JsonIgnore
  public ResourceBundle get() {
    return resourceBundle;
  }

  public void setLocale(Locale locale) {
    resourceBundle = ResourceBundle.getBundle(PIPELINE_CONTAINER_BUNDLE, locale);
  }

  public List<Issue> getPipelineIssues() {
    return pipeline;
  }

  public Map<String, List<StageIssue>> getStageIssues() {
    return stages;
  }

  public boolean hasIssues() {
    return !pipeline.isEmpty() || !!stages.isEmpty();
  }

}
