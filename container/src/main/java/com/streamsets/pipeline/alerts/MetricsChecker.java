/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.MetricDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MetricsChecker {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsChecker.class);
  private static final String USER_PREFIX = "user.";

  private final MetricDefinition metricDefinition;
  private final MetricRegistry metrics;
  private final ELEvaluator.Variables variables;
  private final ELEvaluator elEvaluator;

  public MetricsChecker(MetricDefinition metricDefinition, MetricRegistry metrics, ELEvaluator.Variables variables,
                        ELEvaluator elEvaluator) {
    this.metricDefinition = metricDefinition;
    this.metrics = metrics;
    this.variables = variables;
    this.elEvaluator = elEvaluator;
  }

  public void recordMetrics(Map<String, List<Record>> snapshot) {
    if(metricDefinition.isEnabled()) {
      List<Record> records = snapshot.get(LaneResolver.getPostFixedLaneForObserver(metricDefinition.getLane()));
      //As of now we know that this definition does not apply to this stage because the snapshot does not
      //have the lane. This will be fixed when we have per stage Observer implementation
      if(records != null && !records.isEmpty()) {
        switch (metricDefinition.getMetricType()) {
          case METER:
            Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + metricDefinition.getId());
            if (meter == null) {
              meter = MetricsConfigurator.createMeter(metrics, USER_PREFIX + metricDefinition.getId());
            }
            recordMeter(meter, records);
            break;
          case HISTOGRAM:
            Histogram histogram = MetricsConfigurator.getHistogram(metrics, USER_PREFIX + metricDefinition.getId());
            if (histogram == null) {
              histogram = MetricsConfigurator.createHistogram5Min(metrics, USER_PREFIX + metricDefinition.getId());
            }
            recordHistogram(histogram, records);
            break;
          default:
            //no-op
        }
      }
    }
  }

  private void recordHistogram(Histogram histogram, List<Record> records) {
    for(Record record : records) {
      boolean success = evaluate(record);
      if (success) {
        histogram.update(1);
      }
    }
  }

  private void recordMeter(Meter meter, List<Record> records) {
    for (Record record : records) {
      boolean success = evaluate(record);
      if (success) {
        meter.mark();
      }
    }
  }

  private boolean evaluate(Record record) {
    try {
      return AlertsUtil.evaluateRecord(record, metricDefinition.getPredicate(), variables, elEvaluator);
    } catch (ObserverException e) {
      //A faulty condition should not take down rest of the alerts with it.
      //Log and it and continue for now
      LOG.error("Error processing metric definition '{}', reason: {}", metricDefinition.getId(), e.getMessage());
      return false;
    }
  }
}
