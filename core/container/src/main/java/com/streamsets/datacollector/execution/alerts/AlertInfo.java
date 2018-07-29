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
package com.streamsets.datacollector.execution.alerts;


import com.codahale.metrics.Gauge;
import com.streamsets.datacollector.config.RuleDefinition;

public class AlertInfo {
  private final String pipelineName;
  private final RuleDefinition ruleDefinition;
  private final Gauge gauge;

  public AlertInfo(String pipelineName, RuleDefinition ruleDefinition, Gauge gauge) {
    this.pipelineName = pipelineName;
    this.ruleDefinition = ruleDefinition;
    this.gauge = gauge;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public RuleDefinition getRuleDefinition() {
    return ruleDefinition;
  }

  public Gauge getGauge() {
    return gauge;
  }
}
