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

package com.streamsets.datacollector.event.dto;

import java.util.HashMap;
import java.util.Map;

public enum EventType {

  VALIDATE_PIPELINE(1000),
  SAVE_PIPELINE(1001),
  SAVE_RULES_PIPELINE(1002),
  START_PIPELINE(1003),
  STOP_PIPELINE(1004),
  RESET_OFFSET_PIPELINE(1005),
  DELETE_PIPELINE(1006),
  DELETE_HISTORY_PIPELINE(1007),
  PING_FREQUENCY_ADJUSTMENT(1008),
  STOP_DELETE_PIPELINE(1009),
  STATUS_PIPELINE(2000),
  SDC_INFO_EVENT(2001),
  ACK_EVENT(5000);

  private final int value;

  private static final Map<Integer, EventType> intToTypeMap = new HashMap<Integer, EventType>();
  static {
    for (EventType eventType : EventType.values()) {
      intToTypeMap.put(eventType.value, eventType);
    }
  }

  private EventType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static EventType fromValue(int x) {
    EventType eventType = intToTypeMap.get(x);
    if (eventType == null) {
      throw new IllegalArgumentException("Cannot find event type from value " + x);
    }
    return eventType;
  }
}
