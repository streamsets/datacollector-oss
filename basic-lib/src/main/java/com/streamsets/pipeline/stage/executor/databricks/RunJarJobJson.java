/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.executor.databricks;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RunJarJobJson {

  private int jobId;
  private List<String> parameters;

  public RunJarJobJson(
      int jobId,
      List<String> parameters
  ) {
    this.jobId = jobId;
    this.parameters = parameters;
  }

  @JsonProperty("job_id")
  public int getJobId() {
    return jobId;
  }

  @JsonProperty("jar_params")
  public List<String> getParameters() {
    return parameters;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }
}
