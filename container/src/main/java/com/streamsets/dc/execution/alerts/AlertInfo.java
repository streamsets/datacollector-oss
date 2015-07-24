/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.alerts;


import com.codahale.metrics.Gauge;
import com.streamsets.pipeline.config.RuleDefinition;

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
