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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RunJobResponseJson {

  private final int numberInJob;
  private final int runId;

  @JsonCreator
  public RunJobResponseJson(
      @JsonProperty("number_in_job") int numberInJob,
      @JsonProperty("run_id") int runId
  ) {
    this.numberInJob = numberInJob;
    this.runId = runId;
  }

  public int getNumberInJob() {
    return numberInJob;
  }

  public int getRunId() {
    return runId;
  }
}
