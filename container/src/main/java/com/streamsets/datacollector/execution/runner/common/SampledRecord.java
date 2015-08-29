/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.pipeline.api.Record;

public class SampledRecord {
  private final Record record;
  private final boolean matchedCondition;

  public SampledRecord(Record record, boolean matchedCondition) {
    this.record = record;
    this.matchedCondition = matchedCondition;
  }

  public Record getRecord() {
    return record;
  }

  public boolean isMatchedCondition() {
    return matchedCondition;
  }
}
