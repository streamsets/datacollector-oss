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

import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.lib.security.acl.dto.Acl;

public class PipelineSaveEvent extends PipelineBaseEvent {

  private PipelineConfigAndRules pipelineConfigurationAndRules;
  private String description;
  private String offset;
  private int offsetProtocolVersion;
  private Acl acl;

  public PipelineSaveEvent() {
  }

  public PipelineConfigAndRules getPipelineConfigurationAndRules() {
    return pipelineConfigurationAndRules;
  }

  public void setPipelineConfigurationAndRules(PipelineConfigAndRules pipelineConfigurationAndRules) {
    this.pipelineConfigurationAndRules = pipelineConfigurationAndRules;
  }

  public int getOffsetProtocolVersion() {
    return offsetProtocolVersion;
  }

  public void setOffsetProtocolVersion(int offsetProtocolVersion) {
    this.offsetProtocolVersion = offsetProtocolVersion;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  public Acl getAcl() {
    return acl;
  }

  public void setAcl(Acl acl) {
    this.acl = acl;
  }
}
