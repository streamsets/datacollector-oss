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
package com.streamsets.pipeline.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.util.Issue;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PreviewPipelineOutput {
  private Locale locale;
  private final List<Issue> issues;
  private final MetricRegistry metrics;
  private final List<StageOutput> stagesOutput;
  private String sourceOffset;
  private String newSourceOffset;

  public PreviewPipelineOutput(List<Issue> issues, PreviewPipelineRunner runner) {
    this.issues = issues;
    this.metrics = runner.getMetrics();
    this.stagesOutput = runner.getStagesOutputSnapshot();
    this.sourceOffset = runner.getSourceOffset();
    this.newSourceOffset = runner.getNewSourceOffset();
  }

  public void setLocale(Locale locale) {
    this.locale = locale;
  }

  public Map<String, List<String>> getIssues() {
    return Issue.getLocalizedMessages(issues, locale);
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public List<StageOutput> getStagesOutput() {
    return stagesOutput;
  }

  public String getSourceOffset() {
    return sourceOffset;
  }

  public String getNewSourceOffset() {
    return newSourceOffset;
  }

}
