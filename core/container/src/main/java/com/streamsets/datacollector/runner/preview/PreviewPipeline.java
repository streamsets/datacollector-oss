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

import com.streamsets.datacollector.execution.runner.common.PipelineStopReason;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.StageException;

import java.util.Collections;
import java.util.List;

public class PreviewPipeline {
  private final String name;
  private final String rev;
  private final Pipeline pipeline;
  private final Issues issues;

  public PreviewPipeline(String name, String rev, Pipeline pipeline, Issues issues) {
    this.name = name;
    this.rev = rev;
    this.issues = issues;
    this.pipeline = pipeline;
  }

  @SuppressWarnings("unchecked")
  public PreviewPipelineOutput run() throws StageException, PipelineRuntimeException{
    return run(Collections.EMPTY_LIST);
  }

  public PreviewPipelineOutput run(List<StageOutput> stageOutputsToOverride)
      throws StageException, PipelineRuntimeException{
    List<Issue> initIssues = pipeline.init(true);
    if (initIssues.isEmpty()) {
      pipeline.run(stageOutputsToOverride);
    } else {
      issues.addAll(initIssues);
      throw new PipelineRuntimeException(issues);
    }
    return new PreviewPipelineOutput(issues, pipeline.getRunner());
  }

  public List<Issue> validateConfigs() throws StageException {
    return pipeline.validateConfigs();
  }

  public void destroy(PipelineStopReason reason) throws StageException, PipelineRuntimeException {
    pipeline.destroy(true, reason);
  }

}
