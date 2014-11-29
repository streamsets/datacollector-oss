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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;

import java.util.List;
import java.util.Map;

public class StageOutput {
  private final String instanceName;
  private final Map<String, List<Record>> output;
  private final List<Record> errorRecords;

  @JsonCreator
  public StageOutput(
      @JsonProperty("instanceName") String instanceName,
      @JsonProperty("output") Map<String, List<Record>> output,
      @JsonProperty("errorRecords") List<Record> errorRecords) {
    this.instanceName = instanceName;
    this.output = output;
    this.errorRecords = errorRecords;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public Map<String, List<Record>> getOutput() {
    return output;
  }

  public List<Record> getErrorRecords() {
    return errorRecords;
  }

  @Override
  public String toString() {
    return Utils.format("StageOutput[instance='{}' lanes='{}']", instanceName, output.keySet());
  }

}
