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
package com.streamsets.datacollector.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.metrics.MetricsEventListener;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventListenerManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventListenerManager.class);
  private final Map<String, List<MetricsEventListener>> metricsEventListenerMap;
  private final List<StateEventListener> stateEventListenerList;
  private final List<AlertEventListener> alertEventListenerList;

  public EventListenerManager() {
    metricsEventListenerMap = new HashMap<>();
    stateEventListenerList = new ArrayList<>();
    alertEventListenerList = new ArrayList<>();
  }

  public void addStateEventListener(StateEventListener stateEventListener) {
    synchronized(stateEventListenerList) {
      stateEventListenerList.add(stateEventListener);
    }
  }

  public List<StateEventListener> getStateEventListenerList() {
    return ImmutableList.copyOf(stateEventListenerList);
  }

  public void removeStateEventListener(StateEventListener stateEventListener) {
    synchronized(stateEventListenerList) {
      stateEventListenerList.remove(stateEventListener);
    }
  }

  public void addMetricsEventListener(String pipelineName, MetricsEventListener metricsEventListener) {
    synchronized (metricsEventListenerMap) {
      List<MetricsEventListener> metricsEventListeners = metricsEventListenerMap.get(pipelineName);
      if(metricsEventListeners == null) {
        metricsEventListeners = new ArrayList<>();
        metricsEventListenerMap.put(pipelineName, metricsEventListeners);
      }
      metricsEventListeners.add(metricsEventListener);
    }
  }

  public void removeMetricsEventListener(String pipelineName, MetricsEventListener metricsEventListener) {
    synchronized (metricsEventListenerMap) {
      if(metricsEventListenerMap.containsKey(pipelineName)) {
        metricsEventListenerMap.get(pipelineName).remove(metricsEventListener);
      }
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

  public boolean hasMetricEventListeners(String pipelineName) {
    return metricsEventListenerMap.get(pipelineName) != null &&  metricsEventListenerMap.get(pipelineName).size() > 0;
  }

  public void broadcastAlerts(AlertInfo alertInfo) {
    if(alertEventListenerList.size() > 0) {
      try {
        List<AlertEventListener> alertEventListenerListCopy;
        synchronized (alertEventListenerList) {
          alertEventListenerListCopy = new ArrayList<>(alertEventListenerList);
        }

        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String ruleDefinitionJSONStr = objectMapper.writer().writeValueAsString(alertInfo);
        for(AlertEventListener alertEventListener : alertEventListenerListCopy) {
          try {
            alertEventListener.notification(ruleDefinitionJSONStr);
          } catch (Exception ex) {
            LOG.warn("Error while notifying alerts, {}", ex.toString(), ex);
          }
        }
      } catch (JsonProcessingException ex) {
        LOG.warn("Error while broadcasting alerts, {}", ex.toString(), ex);
      }
    }
  }

  public void broadcastStateChange(
      PipelineState fromState,
      PipelineState toState,
      ThreadUsage threadUsage,
      Map<String, String> offset
  ) {
    if(stateEventListenerList.size() > 0) {
      List<StateEventListener> stateEventListenerListCopy;
      synchronized (stateEventListenerList) {
        stateEventListenerListCopy = new ArrayList<>(stateEventListenerList);
      }

      try {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String toStateJson = objectMapper.writer().writeValueAsString(BeanHelper.wrapPipelineState(toState, true));

        for(StateEventListener stateEventListener : stateEventListenerListCopy) {
          try {
            stateEventListener.onStateChange(fromState, toState, toStateJson, threadUsage, offset);
          } catch(Exception ex) {
            LOG.warn("Error while broadcasting Pipeline State, {}", ex.toString(), ex);
          }
        }
      } catch (JsonProcessingException ex) {
        LOG.warn("Error while broadcasting Pipeline State, {}", ex.toString(), ex);
      }
    }
  }

  public void broadcastMetrics(String pipelineName, String metricsJSONStr) {
    if(metricsEventListenerMap.containsKey(pipelineName) && metricsEventListenerMap.get(pipelineName).size() > 0) {
      List<MetricsEventListener> metricsEventListenerListCopy;
      synchronized (metricsEventListenerMap) {
        metricsEventListenerListCopy = new ArrayList<>(metricsEventListenerMap.get(pipelineName));
      }

      for(MetricsEventListener metricsEventListener : metricsEventListenerListCopy) {
        try {
          metricsEventListener.notification(metricsJSONStr);
        } catch(Exception ex) {
          LOG.warn("Error while notifying metrics, {}", ex.toString(), ex);
        }
      }
    }
  }

}
