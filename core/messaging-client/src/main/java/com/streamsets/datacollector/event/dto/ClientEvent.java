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

import java.util.List;

public class ClientEvent {
  private final String eventId;
  private final List<String> destinations;
  private final boolean requiresAck;
  private final boolean isAckEvent;
  private final EventType eventType;
  private final Event event;
  private final String orgId;
  private final int eventTypeId;

  public ClientEvent(String eventId,
    List<String> destinations,
    boolean requiresAck,
    boolean isAckEvent,
    EventType eventType,
    Event event,
    String orgId) {
    this.eventId = eventId;
    this.destinations = destinations;
    this.requiresAck = requiresAck;
    this.eventType = eventType;
    this.eventTypeId = eventType.getValue();
    this.event = event;
    this.isAckEvent = isAckEvent;
    this.orgId = orgId;
  }

  public String getEventId() {
    return eventId;
  }

  public boolean isAckEvent() {
    return isAckEvent;
  }

  public List<String> getDestinations() {
    return destinations;
  }

  public boolean isRequiresAck() {
    return requiresAck;
  }

  public EventType getEventType() {
    return eventType;
  }

  public Event getEvent() {
    return event;
  }

  public String getOrgId() {
    return orgId;
  }

  public int getEventTypeId() {
    return eventTypeId;
  }
}
