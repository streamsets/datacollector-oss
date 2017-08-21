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
package com.streamsets.datacollector.runner.production;

import com.streamsets.pipeline.api.Record;

import java.util.List;
import java.util.Map;

public class DataRulesEvaluationRequest {

  private final Map<String, Map<String, List<Record>>> snapshot;
  private final Map<String, Integer> laneToRecordsSize;
  public DataRulesEvaluationRequest(Map<String, Map<String, List<Record>>> snapshot, Map<String, Integer> laneToRecordsSize) {
    this.snapshot = snapshot;
    this.laneToRecordsSize = laneToRecordsSize;
  }

  public Map<String, Map<String, List<Record>>> getSnapshot() {
    return snapshot;
  }

  public Map<String, Integer> getLaneToRecordsSize() {
    return laneToRecordsSize;
  }
}
