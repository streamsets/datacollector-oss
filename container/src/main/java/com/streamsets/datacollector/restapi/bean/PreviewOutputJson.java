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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.preview.common.PreviewOutputImpl;
import com.streamsets.datacollector.runner.StageOutput;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PreviewOutputJson {

  private final PreviewOutput previewOutput;

  @JsonCreator
  public PreviewOutputJson(
      @JsonProperty("status") PreviewStatus status,
      @JsonProperty("issues") IssuesJson issues,
      @JsonProperty("batchesOutput") List<List<StageOutputJson>> batchesOutput,
      @JsonProperty("message") String message,
      @JsonProperty("errorStackTrace") String errorStackTrace,
      @JsonProperty("antennaDoctorMessages") List<AntennaDoctorMessageJson> antennaDoctorMessages
  ) {
    List<List<StageOutput>> output = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(batchesOutput)) {
      output.addAll(batchesOutput.stream().map(BeanHelper::unwrapStageOutput).collect(Collectors.toList()));
    }
    previewOutput = new PreviewOutputImpl(
        status,
        issues != null ? issues.getIssues() : null,
        output,
        message,
        errorStackTrace,
        BeanHelper.unwrapAntennaDoctorMessages(antennaDoctorMessages)
    );
  }

  PreviewOutputJson(PreviewOutput previewOutput) {
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

  public String getErrorStackTrace() {
    return previewOutput.getErrorStackTrace();
  }

  public List<AntennaDoctorMessageJson> getAntennaDoctorMessages() {
    return BeanHelper.wrapAntennaDoctorMessages(previewOutput.getAntennaDoctorMessages());
  }

  @JsonIgnore
  public PreviewOutput getPreviewOutput() {
    return previewOutput;
  }
}
