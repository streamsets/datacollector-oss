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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;
import java.util.Map;

public class StageOutput {
  private final String instanceName;
  private final Map<String, List<Record>> output;
  private final List<Record> errorRecords;
  private final List<ErrorMessage> stageErrors;
  private final List<Record> eventRecords;

  @SuppressWarnings("unchecked")
  public StageOutput(
      String instanceName,
      Map<String, List<Record>> output,
      ErrorSink errorSink,
      EventSink eventSink
  ) throws StageException {
    this(
        instanceName,
        output,
        errorSink.getErrorRecords(instanceName),
        errorSink.getStageErrors().get(instanceName),
        eventSink.getStageEvents(instanceName)
    );
  }

  public StageOutput(
      String instanceName,
      Map<String, List<Record>> output,
      List<Record> errorRecords,
      List<ErrorMessage> stageErrors,
      List<Record> eventRecords
  ) {
    this.instanceName = instanceName;
    this.output = output;
    this.errorRecords = errorRecords;
    this.stageErrors = stageErrors;
    this.eventRecords = eventRecords;
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

  public List<ErrorMessage> getStageErrors() {
    return stageErrors;
  }

  public List<Record> getEventRecords() {
    return eventRecords;
  }

  @Override
  public String toString() {
    return Utils.format("StageOutput[instance='{}' lanes='{}']", instanceName, output.keySet());
  }

}
