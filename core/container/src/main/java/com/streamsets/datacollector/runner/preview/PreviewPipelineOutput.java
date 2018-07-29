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
package com.streamsets.datacollector.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issues;

import java.util.List;

public class PreviewPipelineOutput {
  private final Issues issues;
  private final MetricRegistry metrics;
  private final List<List<StageOutput>> batchesOutput;

  public PreviewPipelineOutput(Issues issues, PipelineRunner runner) {
    this.issues = issues;
    this.metrics = runner.getMetrics();
    this.batchesOutput = runner.getBatchesOutput();
  }

  public Issues getIssues() {
    return issues;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public List<List<StageOutput>> getBatchesOutput() {
    return batchesOutput;
  }

}
