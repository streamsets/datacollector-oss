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
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.ErrorId;
import com.streamsets.pipeline.util.PipelineException;
import com.streamsets.pipeline.validation.Issue;

import java.util.Collections;
import java.util.List;

public class PipelineRuntimeException extends PipelineException {


  public enum ERROR implements ErrorId {
    PIPELINE_CONFIGURATION("Pipeline configuration error, {}"),
    PIPELINE_BUILD("Pipeline build error, {}"),
    STAGE_CONFIG_INJECTION("Stage '{}', instance '{}', variable '{}', value '{}', configuration injection error, {}"),
    STAGE_MISSING_CONFIG("Stage '{}', instance '{}', missing configuration '{}'"),
    CANNOT_PREVIEW("Cannot preview, {}"),
    INVALID_REQUIRED_FIELDS_CONFIG_VALUE("Instance '{}', required fields configuration must be a List, it is a '{}'"),
    INVALID_INSTANCE_STAGE("Invalid instance '{}'"),
    CANNOT_PREVIEW_STAGE_ON_SOURCE("Cannot do a preview stage run on a source, instance '{}'"),
    CANNOT_RUN("Cannot run, {}"),
    CANNOT_RAW_SOURCE_PREVIEW_EMPTY_PIPELINE("Cannot do a raw source preview as the pipeline '{}' is empty"),
    CANNOT_RAW_SOURCE_PREVIEW("Cannot do a raw source preview on source as the following required parameters are not supplied : {}");

    private final String msg;

    ERROR(String msg) {
      this.msg = msg;
    }

    @Override
    public String getMessage() {
      return msg;
    }

  }

  private final List<Issue> issues;

  public PipelineRuntimeException(ERROR id, List<Issue> issues) {
    super(id, issues);
    this.issues = issues;
  }

  @SuppressWarnings("unchecked")
  public PipelineRuntimeException(ERROR id, Object... params) {
    super(id, params);
    issues = Collections.EMPTY_LIST;
  }

  public List<Issue> getIssues() {
    return issues;
  }

}
