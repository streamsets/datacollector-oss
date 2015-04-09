/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MetricsEventRunnable implements Runnable {
  private final static Logger LOG = LoggerFactory.getLogger(MetricsEventRunnable.class);
  private List<MetricsEventListener> metricsEventListenerList = new ArrayList<>();

  private final ProductionPipelineManagerTask pipelineManager;

  public MetricsEventRunnable(ProductionPipelineManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  public void addMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventListenerList.add(metricsEventListener);
  }

  public void removeMetricsEventListener(MetricsEventListener metricsEventListener) {
    metricsEventListenerList.remove(metricsEventListener);
  }

  public void run() {
    try {
      if (metricsEventListenerList.size() > 0 && pipelineManager.getPipelineState() != null &&
        pipelineManager.getPipelineState().getState() == State.RUNNING) {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String metricsJSONStr = objectMapper.writer().writeValueAsString(pipelineManager.getMetrics());
        for(MetricsEventListener alertEventListener : metricsEventListenerList) {
          try {
            alertEventListener.notification(metricsJSONStr);
          } catch(Exception ex) {
            LOG.warn("Error while notifying metrics, {}", ex.getMessage(), ex);
          }
        }
      }
    } catch (IOException ex) {
      LOG.warn("Error while serializing metrics, {}", ex.getMessage(), ex);
    }
  }
}
