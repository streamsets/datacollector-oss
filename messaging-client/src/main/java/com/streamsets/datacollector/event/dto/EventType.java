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
package com.streamsets.datacollector.event.dto;

import java.util.HashMap;
import java.util.Map;

public enum EventType {

  // EVENT CODE for DPM->SDC events: 1000 - 1999
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
  SSO_DISCONNECTED_MODE_CREDENTIALS(1010),
  SYNC_ACL(1011),
  BLOB_STORE(1012),
  BLOB_DELETE(1013),
  BLOB_DELETE_VERSION(1014),
  SAVE_CONFIGURATION(1015),
  PREVIEW_PIPELINE(1016),
  DYNAMIC_PREVIEW(1017),

  // EVENT CODE for SDC->DPM events: 2000 - 2999
  STATUS_PIPELINE(2000),
  SDC_INFO_EVENT(2001),
  STATUS_MULTIPLE_PIPELINES(2002),
  SDC_PROCESS_METRICS_EVENT(2003),

  // EVENT CODE FOR ACK events: >=5000
  ACK_EVENT(5000);

  private final int value;

  private static final Map<Integer, EventType> intToTypeMap = new HashMap<>();
  static {
    for (EventType eventType : EventType.values()) {
      intToTypeMap.put(eventType.value, eventType);
    }
  }

  EventType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static EventType fromValue(int x) {
    EventType eventType = intToTypeMap.get(x);
    return eventType;
  }
}
