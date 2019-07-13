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
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.AntennaDoctorMessage;

import java.util.List;

public interface PreviewOutput {

  public PreviewStatus getStatus();

  public Issues getIssues();

  public List<List<StageOutput>> getOutput();

  public String getMessage();

  public String getErrorStackTrace();

  public List<AntennaDoctorMessage> getAntennaDoctorMessages();
}
