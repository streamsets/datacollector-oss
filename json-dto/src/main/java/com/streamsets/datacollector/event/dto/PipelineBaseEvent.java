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

// Events for all commands requiring Pipeline name, rev and user
// made abstract under SDC-10218, when Jackson polymorphism was introduced
public abstract class PipelineBaseEvent implements Event {

  private String name;
  private String rev;
  private String user;

  public PipelineBaseEvent() {
  }

  public PipelineBaseEvent(String name, String rev, String user) {
    this.name = name;
    this.rev = rev;
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getName() {
    return name;
  }

  public String getRev() {
    return rev;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setRev(String rev) {
    this.rev = rev;
  }

}
