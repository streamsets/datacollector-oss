/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;

public class SampledRecordJson {
  private final SampledRecord sampledRecord;

  public SampledRecordJson(SampledRecord sampledRecord) {
    this.sampledRecord = sampledRecord;
  }

  public RecordJson getRecord() {
    return BeanHelper.wrapRecord(sampledRecord.getRecord());
  }

  public boolean isMatchedCondition() {
    return sampledRecord.isMatchedCondition();
  }

  @JsonIgnore
  public SampledRecord getSampledRecord() {
    return sampledRecord;
  }
}
