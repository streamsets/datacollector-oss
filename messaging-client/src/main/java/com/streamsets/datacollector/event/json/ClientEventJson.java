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
package com.streamsets.datacollector.event.json;

import java.util.List;

public class ClientEventJson {
  private String eventId;
  private List<String> destinations;
  private boolean requiresAck;
  private boolean isAckEvent;
  private int eventTypeId;
  private String payload;
  private String orgId;

  public ClientEventJson() {
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public List<String> getDestinations() {
    return destinations;
  }

  public void setDestinations(List<String> destinations) {
    this.destinations = destinations;
  }

  public boolean isRequiresAck() {
    return requiresAck;
  }

  public void setRequiresAck(boolean requiresAck) {
    this.requiresAck = requiresAck;
  }

  public boolean isAckEvent() {
    return isAckEvent;
  }

  public void setAckEvent(boolean isAckEvent) {
    this.isAckEvent = isAckEvent;
  }

  public int getEventTypeId() {
    return eventTypeId;
  }

  public void setEventTypeId(int eventTypeId) {
    this.eventTypeId = eventTypeId;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public String getOrgId() {
    return orgId;
  }

  public void setOrgId(String orgId) {
    this.orgId = orgId;
  }

  @Override
  public int hashCode() {
    return eventId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (obj instanceof ClientEventJson) {
      ClientEventJson other = (ClientEventJson) obj;
      return this.eventId.equals(other.getEventId());
    }
    return false;
  }
}
