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
package com.streamsets.pipeline.prodmanager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineState {
  private final String name;
  private final String rev;
  private final State state;
  private final String message;
  private final long lastStatusChange;

  @JsonCreator
  public PipelineState(
      @JsonProperty("name") String name,
      @JsonProperty("rev") String rev,
      @JsonProperty("state") State state,
      @JsonProperty("message") String message,
      @JsonProperty("lastStatusChange") long lastStatusChange) {
    this.name = name;
    this.rev = rev;
    this.state = state;
    this.message = message;
    this.lastStatusChange = lastStatusChange;
  }

  public String getRev() {
    return rev;
  }

  public State getState() {
    return this.state;
  }

  public String getMessage() {
    return this.message;
  }

  public long getLastStatusChange() {
    return lastStatusChange;
  }

  public String getName() {
    return name;
  }
}
