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

public class PipelineSaveRulesEvent extends PipelineBaseEvent {

  private String ruleDefinitions;

  public PipelineSaveRulesEvent() {
  }

  public PipelineSaveRulesEvent(String name, String rev, String user, String ruleDefinitions) {
    super(name, rev, user);
    this.ruleDefinitions = ruleDefinitions;
  }

  public void setRuleDefinitions(String ruleDefinitions) {
    this.ruleDefinitions = ruleDefinitions;
  }

  public String getRuleDefinitions() {
    return ruleDefinitions;
  }
}
