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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.streamsets.datacollector.execution.PreviewStatus;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PreviewInfoJson {
  private String previewerId;
  private PreviewStatus status;
  private String pipelineId;
  private Map<String, Object> attributes;

  public PreviewInfoJson() {
  }

  public PreviewInfoJson(
      String previewerId,
      PreviewStatus status,
      String pipelineId,
      Map<String, Object> attributes
  ) {
    this.previewerId = previewerId;
    this.status = status;
    this.pipelineId = pipelineId;
    this.attributes = attributes;
  }

  public String getPreviewerId() {
    return previewerId;
  }

  public void setPreviewerId(String previewerId) {
    this.previewerId = previewerId;
  }

  public PreviewStatus getStatus() {
    return status;
  }

  public void setStatus(PreviewStatus previewStatus) {
    this.status = previewStatus;
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public void setPipelineId(String pipelineId) {
    this.pipelineId = pipelineId;
  }

  public Map<String, Object> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, Object> attributes) {
    this.attributes = attributes;
  }
}
