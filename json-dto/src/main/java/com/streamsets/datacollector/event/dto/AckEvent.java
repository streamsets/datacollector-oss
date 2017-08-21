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

public class AckEvent implements Event {

  private AckEventStatus ackEventStatus;
  private String message;

  public AckEvent() {
  }

  public AckEvent(AckEventStatus ackEventStatus, String message) {
    this.ackEventStatus = ackEventStatus;
    this.message = message;
  }

  public AckEventStatus getAckEventStatus() {
    return ackEventStatus;
  }

  public void setAckEventStatus(AckEventStatus ackEventStatus) {
    this.ackEventStatus = ackEventStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

}
