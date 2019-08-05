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
package com.streamsets.datacollector.execution.preview.common;

import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.AntennaDoctorMessage;
import com.streamsets.pipeline.api.StageException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class PreviewOutputImpl implements PreviewOutput {

  private final PreviewStatus previewStatus;
  private final Issues issues;
  private final List<List<StageOutput>> output;
  private final String message;
  private final String errorStackTrace;
  private final List<AntennaDoctorMessage> antennaDoctorMessages;

  public PreviewOutputImpl(
    PreviewStatus previewStatus,
    Issues issues,
    List<List<StageOutput>> output
  ) {
    this.previewStatus = previewStatus;
    this.issues = issues;
    this.output = output;
    this.message = null;
    this.errorStackTrace = null;
    this.antennaDoctorMessages = null;
  }

  public PreviewOutputImpl(
    PreviewStatus previewStatus,
    Issues issues,
    Throwable e
  ) {
    this.previewStatus = previewStatus;
    this.issues = issues;
    this.output = null;
    this.message = e.toString();
    this.errorStackTrace = stackTraceToString(e);
    this.antennaDoctorMessages = e instanceof StageException ? ((StageException) e).getAntennaDoctorMessages() : null;
  }

  public PreviewOutputImpl(
    PreviewStatus previewStatus,
    Throwable e
  ) {
    this.previewStatus = previewStatus;
    this.issues = null;
    this.output = null;
    this.message = e.toString();
    this.errorStackTrace = stackTraceToString(e);
    this.antennaDoctorMessages = e instanceof StageException ? ((StageException) e).getAntennaDoctorMessages() : null;
  }

  public PreviewOutputImpl(
      PreviewStatus previewStatus,
      Issues issues,
      List<List<StageOutput>> output,
      String message,
      String errorStackTrace,
      List<AntennaDoctorMessage> antennaDoctorMessages
  ) {
    this.previewStatus = previewStatus;
    this.issues = issues;
    this.output = output;
    this.message = message;
    this.errorStackTrace = errorStackTrace;
    this.antennaDoctorMessages = antennaDoctorMessages;
  }

  @Override
  public PreviewStatus getStatus() {
    return previewStatus;
  }

  @Override
  public Issues getIssues() {
    return issues;
  }

  @Override
  public List<List<StageOutput>> getOutput() {
    return output;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getErrorStackTrace() {
    return errorStackTrace;
  }

  @Override
  public List<AntennaDoctorMessage> getAntennaDoctorMessages() {
    return antennaDoctorMessages;
  }

  private String stackTraceToString(Throwable e) {
    StringWriter writer = new StringWriter();
    PrintWriter printWriter = new PrintWriter(writer);
    e.printStackTrace(printWriter);
    printWriter.close();

    return writer.toString();
  }

}
