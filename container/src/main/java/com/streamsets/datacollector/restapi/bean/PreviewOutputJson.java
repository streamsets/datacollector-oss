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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;

import java.util.List;

public class PreviewOutputJson {

  private final PreviewOutput previewOutput;

  public PreviewOutputJson(PreviewOutput previewOutput) {
    this.previewOutput = previewOutput;
  }

  public PreviewStatus getStatus() {
    return previewOutput.getStatus();
  }

  public IssuesJson getIssues() {
    return BeanHelper.wrapIssues(previewOutput.getIssues());
  }

  public List<List<StageOutputJson>> getBatchesOutput() {
    return BeanHelper.wrapStageOutputLists(previewOutput.getOutput());
  }

  public String getMessage() {
    return previewOutput.getMessage();
  }

  @JsonIgnore
  public PreviewOutput getPreviewOutput() {
    return previewOutput;
  }
}
