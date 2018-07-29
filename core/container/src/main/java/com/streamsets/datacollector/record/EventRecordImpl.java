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
package com.streamsets.datacollector.record;

import com.streamsets.pipeline.api.EventRecord;

public class EventRecordImpl extends RecordImpl implements EventRecord {

  public EventRecordImpl(
      String type,
      int version,
      String stageCreator,
      String recordSourceId,
      byte[] raw,
      String rawMime
  ) {
    super(stageCreator, recordSourceId, raw, rawMime);
    this.addStageToStagePath(stageCreator);
    this.createTrackingId();
    setEventAtributes(type, version);
  }

  private EventRecordImpl(RecordImpl record) {
    super(record);
  }

  private void setEventAtributes(String type, int version) {
    getHeader().setAttribute(EventRecord.TYPE, type);
    getHeader().setAttribute(EventRecord.VERSION, String.valueOf(version));
    getHeader().setAttribute(EventRecord.CREATION_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
  }


  @Override
  public EventRecordImpl clone() {
    return new EventRecordImpl(this);
  }


  @Override
  public String getEventType() {
    return getHeader().getAttribute(EventRecord.TYPE);
  }

  @Override
  public String getEventVersion() {
    return getHeader().getAttribute(EventRecord.VERSION);
  }

  @Override
  public String getEventCreationTimestamp() {
    return getHeader().getAttribute(EventRecord.CREATION_TIMESTAMP);
  }
}
