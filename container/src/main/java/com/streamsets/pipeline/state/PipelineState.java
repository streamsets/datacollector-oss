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
package com.streamsets.pipeline.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class PipelineState {
  private final String pipelineName;
  private final State pipelineState;
  private final List<String> errors;

  @JsonCreator
  public PipelineState(
      @JsonProperty("name") String pipelineName,
      @JsonProperty("pipelineState") State pipelineState,
      @JsonProperty("errors") List<String> errors) {
    this.pipelineName = pipelineName;
    this.pipelineState = pipelineState;
    this.errors = errors;
  }

  public String getPipelineName() {
    return this.pipelineName;
  }

  public State getPipelineState() {
    return this.pipelineState;
  }

  public List<String> getErrors() {
    return this.errors;
  }

}
