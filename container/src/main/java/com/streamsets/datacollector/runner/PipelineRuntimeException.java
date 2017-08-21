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
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.ErrorCode;

public class PipelineRuntimeException extends PipelineException {
  private final Issues issues;

  @SuppressWarnings("unchecked")
  public PipelineRuntimeException(ErrorCode errorCode, Object... params) {
    super(errorCode, params);
    issues = null;
  }

  public PipelineRuntimeException(Issues issues) {
    super(ContainerError.CONTAINER_0165, issues.getIssues());
    this.issues = issues;
  }

  public Issues getIssues() {
    return issues;
  }

}
