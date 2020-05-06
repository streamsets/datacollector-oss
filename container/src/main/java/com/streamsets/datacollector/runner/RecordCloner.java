/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;

/**
 * This is helpful method to make sure that all places that clone records in the framework will do so the same way.
 */
public class RecordCloner {
  /**
   * Should be directly taken from the associated stage's metadata (@StageDef)
   */
  private final boolean recordByRef;

  public RecordCloner(boolean recordByRef) {
    this.recordByRef = recordByRef;
  }

  public RecordImpl cloneRecordIfNeeded(Record record) {
    return recordByRef ? (RecordImpl)record: ((RecordImpl) record).clone();
  }

  public EventRecordImpl cloneEventIfNeeded(EventRecord record) {
    return recordByRef ? (EventRecordImpl)record : ((EventRecordImpl) record).clone();
  }

}
