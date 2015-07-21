/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.dc.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.dc.execution.alerts.AlertManager;
import com.streamsets.dc.execution.metrics.MetricsEventRunnable;
import com.streamsets.pipeline.alerts.AlertEventListener;
import com.streamsets.pipeline.config.RuleDefinition;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.metrics.MetricsEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public abstract  class AbstractRunner implements Runner {
  @Inject
  protected EventListenerManager eventListenerManager;

  @Override
  public void addStateEventListener(StateEventListener stateEventListener) {
    if(eventListenerManager != null) {
      eventListenerManager.addStateEventListener(stateEventListener);
    }
  }

  @Override
  public void removeStateEventListener(StateEventListener stateEventListener) {
    if(eventListenerManager != null) {
      eventListenerManager.removeStateEventListener(stateEventListener);
    }
  }

  @Override
  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    if(eventListenerManager != null) {
      eventListenerManager.addMetricsEventListener(metricsEventListener);
    }
  }

  @Override
  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    if(eventListenerManager != null) {
      eventListenerManager.removeMetricsEventListener(metricsEventListener);
    }
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    if(eventListenerManager != null) {
      eventListenerManager.addAlertEventListener(alertEventListener);
    }
  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    if(eventListenerManager != null) {
      eventListenerManager.removeAlertEventListener(alertEventListener);
    }
  }
}
