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
import com.streamsets.pipeline.container.PipelineException;
import com.streamsets.pipeline.util.Issue;

import java.util.Collections;
import java.util.List;

public class PipelineRuntimeException extends PipelineException {

  public enum ERROR implements ErrorId {
    PIPELINE_CONFIGURATION("Pipeline configuration error, {}"),
    PIPELINE_BUILD("Pipeline build error, {}"),
    STAGE_CONFIG_INJECTION("Stage '{}', instance '{}', variable '{}', value '{}', configuration injection error, {}");

    private String msg;

    private ERROR(String msg) {
      this.msg = msg;
    }
    @Override
    public String getMessageTemplate() {
      return msg;
    }

  }

  private final List<Issue> issues;

  protected PipelineRuntimeException(ErrorId id, List<Issue> issues) {
    super(id, issues);
    this.issues = issues;
  }

  @SuppressWarnings("unchecked")
  protected PipelineRuntimeException(ErrorId id, Object... params) {
    super(id, params);
    issues = Collections.EMPTY_LIST;
  }

  public List<Issue> getIssues() {
    return issues;
  }

}
