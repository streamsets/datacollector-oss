/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.metrics.MetricsEventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EventListenerManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventListenerManager.class);
  private final List<MetricsEventListener> metricsEventListenerList;
  private final List<StateEventListener> stateEventListenerList;
  private final List<AlertEventListener> alertEventListenerList;

  public EventListenerManager() {
    metricsEventListenerList = new ArrayList<>();
    stateEventListenerList = new ArrayList<>();
    alertEventListenerList = new ArrayList<>();
  }

  public void addStateEventListener(StateEventListener stateEventListener) {
    synchronized(stateEventListenerList) {
      stateEventListenerList.add(stateEventListener);
    }
  }

  public void removeStateEventListener(StateEventListener stateEventListener) {
    synchronized(stateEventListenerList) {
      stateEventListenerList.remove(stateEventListener);
    }
  }

  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    synchronized (metricsEventListenerList) {
      metricsEventListenerList.add(metricsEventListener);
    }
  }

  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    synchronized (metricsEventListenerList) {
      metricsEventListenerList.remove(metricsEventListener);
    }
  }

  public void addAlertEventListener(AlertEventListener alertEventListener) {
    synchronized (alertEventListenerList) {
      alertEventListenerList.add(alertEventListener);
    }
  }

  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    synchronized (alertEventListenerList) {
      alertEventListenerList.remove(alertEventListener);
    }
  }

  public List<MetricsEventListener> getMetricsEventListenerList() {
    return metricsEventListenerList;
  }

  public void broadcastAlerts(AlertInfo alertInfo) {
    if(alertEventListenerList.size() > 0) {
      try {
        List<AlertEventListener> alertEventListenerListCopy;
        synchronized (alertEventListenerList) {
          alertEventListenerListCopy = new ArrayList(alertEventListenerList);
        }

        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String ruleDefinitionJSONStr = objectMapper.writer().writeValueAsString(alertInfo);
        for(AlertEventListener alertEventListener : alertEventListenerListCopy) {
          try {
            alertEventListener.notification(ruleDefinitionJSONStr);
          } catch (Exception ex) {
            LOG.warn("Error while notifying alerts, {}", ex.getMessage(), ex);
          }
        }
      } catch (JsonProcessingException ex) {
        LOG.warn("Error while broadcasting alerts, {}", ex.getMessage(), ex);
      }
    }
  }

  public void broadcastPipelineState(PipelineState pipelineState) {
    if(stateEventListenerList.size() > 0) {
      List<StateEventListener> stateEventListenerListCopy;
      synchronized (stateEventListenerList) {
        stateEventListenerListCopy = new ArrayList(stateEventListenerList);
      }

      try {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String pipelineStateJSONStr = objectMapper.writer().writeValueAsString(pipelineState);

        for(StateEventListener stateEventListener : stateEventListenerListCopy) {
          try {
            stateEventListener.notification(pipelineStateJSONStr);
          } catch(Exception ex) {
            LOG.warn("Error while broadcasting Pipeline State, {}", ex.getMessage(), ex);
          }
        }
      } catch (JsonProcessingException ex) {
        LOG.warn("Error while broadcasting Pipeline State, {}", ex.getMessage(), ex);
      }
    }
  }

  public void broadcastMetrics(String metricsJSONStr) {
    if(metricsEventListenerList.size() > 0) {
      List<MetricsEventListener> metricsEventListenerListCopy;
      synchronized (metricsEventListenerList) {
        metricsEventListenerListCopy = new ArrayList(metricsEventListenerList);
      }

      for(MetricsEventListener metricsEventListener : metricsEventListenerListCopy) {
        try {
          metricsEventListener.notification(metricsJSONStr);
        } catch(Exception ex) {
          LOG.warn("Error while notifying metrics, {}", ex.getMessage(), ex);
        }
      }
    }
  }

}
