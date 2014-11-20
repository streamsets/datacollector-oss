/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class PipelineRevInfo {
  private final Date date;
  private final String user;
  private final String rev;
  private final String tag;
  private final String description;
  private final boolean valid;

  public PipelineRevInfo(PipelineInfo info) {
    date = info.getLastModified();
    user = info.getLastModifier();
    rev = info.getLastRev();
    tag = null;
    description = null;
    valid = info.isValid();
  }

  @JsonCreator
  public PipelineRevInfo(
      @JsonProperty("date") Date date,
      @JsonProperty("user") String user,
      @JsonProperty("rev") String rev,
      @JsonProperty("tag") String tag,
      @JsonProperty("description") String description,
      @JsonProperty("valid") boolean valid) {
    this.date = date;
    this.user = user;
    this.rev = rev;
    this.tag = tag;
    this.description = description;
    this.valid = valid;
  }

  public Date getDate() {
    return date;
  }

  public String getUser() {
    return user;
  }

  public String getRev() {
    return rev;
  }

  public String getTag() {
    return tag;
  }

  public String getDescription() {
    return description;
  }

  public boolean isValid() {
    return valid;
  }

}
