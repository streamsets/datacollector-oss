/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DataRuleEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(DataRuleEvaluator.class);
  private static final String USER_PREFIX = "user.";

  private final MetricRegistry metrics;
  private final ELEvaluator.Variables variables;
  private final ELEvaluator elEvaluator;
  private final List<String> emailIds;
  private final Configuration configuration;
  private final DataRuleDefinition dataRuleDefinition;
  private final AlertManager alertManager;

  public DataRuleEvaluator(MetricRegistry metrics, ELEvaluator.Variables variables, ELEvaluator elEvaluator,
                           AlertManager alertManager, List<String> emailIds, DataRuleDefinition dataRuleDefinition,
                           Configuration configuration) {
    this.metrics = metrics;
    this.variables = variables;
    this.elEvaluator = elEvaluator;
    this.emailIds = emailIds;
    this.dataRuleDefinition = dataRuleDefinition;
    this.configuration = configuration;
    this.alertManager = alertManager;
  }

  public void evaluateRule(int allRecordsSize, List<Record> sampleRecords, String lane,
                           Map<String, EvictingQueue<Record>> ruleToSampledRecordsMap) {

    if (dataRuleDefinition.isEnabled()) {
      int numberOfRecords = (int) Math.floor(allRecordsSize * dataRuleDefinition.getSamplingPercentage() / 100);

      //cache all sampled records for this data rule definition in an evicting queue
      List<Record> sampleSet = sampleRecords.subList(0, numberOfRecords);
      EvictingQueue<Record> sampledRecords = ruleToSampledRecordsMap.get(dataRuleDefinition.getId());
      if (sampledRecords == null) {
        int maxSize = configuration.get(
          com.streamsets.pipeline.prodmanager.Configuration.SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY,
          com.streamsets.pipeline.prodmanager.Configuration.SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT);
        int size = dataRuleDefinition.getSamplingRecordsToRetain();
        if(size > maxSize) {
          size = maxSize;
        }
        sampledRecords = EvictingQueue.create(size);
        ruleToSampledRecordsMap.put(dataRuleDefinition.getId(), sampledRecords);
      }
      //Meter
      //evaluate sample set of records for condition
      int matchingRecordCount = 0;
      int evaluatedRecordCount = 0;
      for (Record r : sampleSet) {
        evaluatedRecordCount++;
        //evaluate
        boolean success = evaluate(r, dataRuleDefinition.getCondition(), dataRuleDefinition.getId());
        if (success) {
          sampledRecords.add(r);
          matchingRecordCount++;
        }
      }

      if (dataRuleDefinition.isAlertEnabled()) {
        //Keep the counters and meters ready before execution
        //batch record counter - cummulative sum of records per batch
        Counter evaluatedRecordCounter = MetricsConfigurator.getCounter(metrics, LaneResolver.getPostFixedLaneForObserver(
            lane));
        if (evaluatedRecordCounter == null) {
          evaluatedRecordCounter = MetricsConfigurator.createCounter(metrics, LaneResolver.getPostFixedLaneForObserver(
              lane));
        }
        //counter for the matching records - cummulative sum of records that match criteria
        Counter matchingRecordCounter = MetricsConfigurator.getCounter(metrics, dataRuleDefinition.getId());
        if (matchingRecordCounter == null) {
          matchingRecordCounter = MetricsConfigurator.createCounter(metrics, dataRuleDefinition.getId());
        }

        evaluatedRecordCounter.inc(evaluatedRecordCount);
        matchingRecordCounter.inc(matchingRecordCount);

        double threshold;
        try {
          threshold = Double.parseDouble(dataRuleDefinition.getThresholdValue());
        } catch (NumberFormatException e) {
          //Soft error for now as we don't want this alert to stop other rules
          LOG.error("Error interpreting threshold '{}' as a number", dataRuleDefinition.getThresholdValue(), e);
          return;
        }
        switch (dataRuleDefinition.getThresholdType()) {
          case COUNT:
            if (matchingRecordCounter.getCount() > threshold) {
              alertManager.alert(matchingRecordCounter.getCount(), emailIds, dataRuleDefinition);
            }
            break;
          case PERCENTAGE:
            if ((matchingRecordCounter.getCount() * 100 / evaluatedRecordCounter.getCount()) > threshold
              && evaluatedRecordCounter.getCount() >= dataRuleDefinition.getMinVolume()) {
              alertManager.alert(matchingRecordCounter.getCount(), emailIds, dataRuleDefinition);
            }
            break;
        }
      }

      if (dataRuleDefinition.isMeterEnabled() && matchingRecordCount > 0) {
        Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
        if (meter == null) {
          meter = MetricsConfigurator.createMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
        }
        meter.mark(matchingRecordCount);
      }
    }
  }

  private boolean evaluate(Record record, String condition, String id) {
    try {
      return AlertsUtil.evaluateRecord(record, condition, variables, elEvaluator);
    } catch (ObserverException e) {
      //A faulty condition should not take down rest of the alerts with it.
      //Log and it and continue for now
      LOG.error("Error processing metric definition '{}', reason: {}", id, e.getMessage(), e);
      return false;
    }
  }

}
