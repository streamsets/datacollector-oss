/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.util.List;

/**
 * Sink for catching all event records.
 */
public class EventSink {
  private List<Record> eventRecords;

  public EventSink() {
    this.eventRecords = new ArrayList<>();
  }

  public void addEvent(EventRecord event) {
    eventRecords.add(event);
  }

  public List<Record> getEventRecords() {
    return eventRecords;
  }

  public void clear() {
    this.eventRecords.clear();
  }
}
