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

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Sink for catching all event records.
 */
public class EventSink {
  private Map<String, List<EventRecord>> eventRecords;

  public EventSink() {
    this.eventRecords = new LinkedHashMap<>();
  }

  public void addEvent(String stage, EventRecord event) {
    List<EventRecord> events = eventRecords.get(stage);
    if(events == null) {
      events = new ArrayList<>();
      eventRecords.put(stage, events);
    }

    events.add(event);
  }

  public List<EventRecord> getStageEventsAsEventRecords(String stage) {
    return eventRecords.containsKey(stage) ? eventRecords.get(stage) : Collections.emptyList();
  }

  public List<Record> getStageEvents(String stage) {
    final List<EventRecord> eventRecords = getStageEventsAsEventRecords(stage);
    final List<Record> records = new LinkedList<>();
    if (eventRecords != null) {
      eventRecords.forEach(r -> records.add(r));
    }
    return records;
  }

  public void clear() {
    this.eventRecords.clear();
  }
}
