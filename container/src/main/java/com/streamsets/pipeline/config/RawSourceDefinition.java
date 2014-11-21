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
package com.streamsets.pipeline.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class RawSourceDefinition {

  private final String rawSourcePreviewerClass;
  private String mime;
  private Map<String, String> previewParams;

  @JsonCreator
  public RawSourceDefinition(
      @JsonProperty("rawSourcePreviewerClass") String rawSourcePreviewerClass,
      @JsonProperty("mime") String mime,
      @JsonProperty("previewParams") Map<String, String> previewParams) {
    this.rawSourcePreviewerClass = rawSourcePreviewerClass;
    this.mime = mime;
    this.previewParams = previewParams;
  }

  public String getRawSourcePreviewerClass() {
    return rawSourcePreviewerClass;
  }

  public String getMime() {
    return mime;
  }

  public Map<String, String> getPreviewParams() {
    return previewParams;
  }

  public void setPreviewParams(Map<String, String> previewParams) {
    this.previewParams = previewParams;
  }

  public void setMime(String mime) {
    this.mime = mime;
  }
}
