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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streamsets.datacollector.runner.production.SourceOffset;

import java.util.Map;

public class SourceOffsetJson {

  private final SourceOffset sourceOffset;

  public SourceOffsetJson() {
    this.sourceOffset = new SourceOffset();
  }

  public SourceOffsetJson(SourceOffset sourceOffset) {
    this.sourceOffset = sourceOffset;
  }

  public SourceOffsetJson(int version, Map<String, String> offsets) {
    this.sourceOffset = new SourceOffset(version, offsets);
  }

  @JsonIgnore
  public String getOffset() {
    return sourceOffset.getOffset();
  }

  public void setOffset(String offset) {
    sourceOffset.setOffset(offset);
  }

  public int getVersion() {
    return sourceOffset.getVersion();
  }

  public void setVersion(int version) {
    sourceOffset.setVersion(version);
  }

  public Map<String, String> getOffsets() {
    return sourceOffset.getOffsets();
  }

  public void setOffsets(Map<String, String> offsets) {
    sourceOffset.setOffsets(offsets);
  }

  @JsonIgnore
  public SourceOffset getSourceOffset() {
    return sourceOffset;
  }
}
