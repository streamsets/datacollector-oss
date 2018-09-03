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

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Sink for catching all event records.
 */
public class EventSink {
  private Map<String, List<EventRecord>> eventRecords;
  private final Map<String, List<? extends Interceptor>> interceptors;

  public EventSink() {
    this.eventRecords = new LinkedHashMap<>();
    interceptors = new HashMap<>();
  }

  public void registerInterceptorsForStage(String stage, List<? extends Interceptor> interceptors) {
    Preconditions.checkState(!this.interceptors.containsKey(stage), Utils.format("Interceptors for stage '{}' already registered", stage));
    this.interceptors.put(stage, interceptors);
  }

  public void addEvent(String stage, EventRecord event) {
    List<EventRecord> events = eventRecords.computeIfAbsent(stage, k -> new ArrayList<>());
    events.add(event);
  }

  public List<EventRecord> getStageEventsAsEventRecords(String stage) throws StageException {
    Preconditions.checkState(interceptors.containsKey(stage), Utils.format("No interceptors registered for stage '{}'", stage));
    return intercept(
      eventRecords.getOrDefault(stage, Collections.emptyList()),
      interceptors.get(stage)
    );
  }

  public List<Record> getStageEvents(String stage) throws StageException {
    final List<EventRecord> eventRecords = getStageEventsAsEventRecords(stage);
    final List<Record> records = new LinkedList<>();
    if (eventRecords != null) {
      records.addAll(eventRecords);
    }
    return records;
  }

  public void clear() {
    this.eventRecords.clear();
  }

  private List<EventRecord> intercept(List<EventRecord> records, List<? extends Interceptor> interceptors) throws StageException {
    for(Interceptor interceptor : interceptors)  {
      records = interceptor.intercept((List)records);
    }

    return records;
  }
}
