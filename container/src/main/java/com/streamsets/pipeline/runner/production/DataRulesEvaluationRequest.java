/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;

import java.util.List;
import java.util.Map;

public class DataRulesEvaluationRequest {

  private final Map<String, List<Record>> snapshot;
  private final Map<String, Integer> laneToRecordsSize;
  public DataRulesEvaluationRequest(Map<String, List<Record>> snapshot, Map<String, Integer> laneToRecordsSize) {
    this.snapshot = snapshot;
    this.laneToRecordsSize = laneToRecordsSize;
  }

  public Map<String, List<Record>> getSnapshot() {
    return snapshot;
  }

  public Map<String, Integer> getLaneToRecordsSize() {
    return laneToRecordsSize;
  }
}
