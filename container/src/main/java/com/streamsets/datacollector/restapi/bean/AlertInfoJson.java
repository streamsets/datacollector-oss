/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

import com.codahale.metrics.Gauge;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.config.RuleDefinition;

public class AlertInfoJson {
  private final AlertInfo alertInfo;

  public AlertInfoJson(AlertInfo alertInfo) {
    this.alertInfo = alertInfo;
  }

  public String getPipelineName() {
    return alertInfo.getPipelineName();
  }

  public RuleDefinition getRuleDefinition() {
    return alertInfo.getRuleDefinition();
  }

  public Gauge getGauge() {
    return alertInfo.getGauge();
  }

}
