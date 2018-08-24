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

import java.util.Map;

public class RunNotebookJobJson {

  private int jobId;
  private Map<String, String> notebookParams;

  public RunNotebookJobJson(
      int jobId,
       Map<String, String> notebookParams
  ) {
    this.jobId = jobId;
    this.notebookParams = notebookParams;
  }

  @JsonProperty("job_id")
  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  @JsonProperty("notebook_params")
  public Map<String, String> getNotebookParams() {
    return notebookParams;
  }

  public void setNotebookParams(Map<String, String> notebookParams) {
    this.notebookParams = notebookParams;
  }
}
