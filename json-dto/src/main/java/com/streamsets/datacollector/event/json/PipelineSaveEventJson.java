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

import com.streamsets.datacollector.config.json.PipelineConfigAndRulesJson;
import com.streamsets.lib.security.acl.json.AclJson;

public class PipelineSaveEventJson extends PipelineBaseEventJson {

  private PipelineConfigAndRulesJson pipelineConfigurationAndRules;
  private String description;
  private String user;
  private String offset;
  private int offsetProtocolVersion;
  private AclJson acl;

  public PipelineConfigAndRulesJson getPipelineConfigurationAndRules() {
    return pipelineConfigurationAndRules;
  }

  public void setPipelineConfigurationAndRules(PipelineConfigAndRulesJson pipelineConfigurationAndRules) {
    this.pipelineConfigurationAndRules = pipelineConfigurationAndRules;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public void setUser(String user) {
    this.user = user;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  public AclJson getAcl() {
    return acl;
  }

  public void setAcl(AclJson acl) {
    this.acl = acl;
  }

  public int getOffsetProtocolVersion() {
    return offsetProtocolVersion;
  }

  public void setOffsetProtocolVersion(int offsetProtocolVersion) {
    this.offsetProtocolVersion = offsetProtocolVersion;
  }
}
