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
