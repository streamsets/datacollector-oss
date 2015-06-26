/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.alerts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELVariables;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;
import com.streamsets.pipeline.el.RuleELRegistry;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.prodmanager.Constants;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ObserverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataRuleEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(DataRuleEvaluator.class);
  private static final String USER_PREFIX = "user.";

  private static final ELEvaluator EL_EVALUATOR = new ELEvaluator("condition", RuleELRegistry.getRuleELs());
  private static final List<String> EL_FUNCTION_IDX = createElFunctionIdx();
  private static final List<String> EL_CONSTANT_IDX = createElConstantIdx();

  private static List<String> createElFunctionIdx() {
    List<ElFunctionDefinition> defs = ELDefinitionExtractor.get().extractFunctions(RuleELRegistry.getRuleELs(),
                                                                                   "DataRules");
    List<String> idx = new ArrayList<>();
    for (ElFunctionDefinition f : defs) {
      idx.add(f.getIndex());
    }
    return idx;
  }

  private static List<String> createElConstantIdx() {
    List<ElConstantDefinition> defs = ELDefinitionExtractor.get().extractConstants(RuleELRegistry.getRuleELs(),
                                                                                   "DataRules");
    List<String> idx = new ArrayList<>();
    for (ElConstantDefinition f : defs) {
      idx.add(f.getIndex());
    }
    return idx;
  }

  private final MetricRegistry metrics;
  private final List<String> emailIds;
  private final Configuration configuration;
  private final DataRuleDefinition dataRuleDefinition;
  private final AlertManager alertManager;

  public DataRuleEvaluator(MetricRegistry metrics, AlertManager alertManager, List<String> emailIds,
      DataRuleDefinition dataRuleDefinition, Configuration configuration) {
    this.metrics = metrics;
    this.emailIds = emailIds;
    this.dataRuleDefinition = dataRuleDefinition;
    this.configuration = configuration;
    this.alertManager = alertManager;
  }

  public void evaluateRule(List<Record> sampleRecords, String lane,
      Map<String, EvictingQueue<Record>> ruleToSampledRecordsMap) {

    if (dataRuleDefinition.isEnabled() && sampleRecords != null && sampleRecords.size() > 0) {
      //cache all sampled records for this data rule definition in an evicting queue
      EvictingQueue<Record> sampledRecords = ruleToSampledRecordsMap.get(dataRuleDefinition.getId());
      if (sampledRecords == null) {
        int maxSize = configuration.get(
            Constants.SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY,
            Constants.SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT);
        int size = dataRuleDefinition.getSamplingRecordsToRetain();
        if (size > maxSize) {
          size = maxSize;
        }
        sampledRecords = EvictingQueue.create(size);
        ruleToSampledRecordsMap.put(dataRuleDefinition.getId(), sampledRecords);
      }
      //Meter
      //evaluate sample set of records for condition
      int matchingRecordCount = 0;
      int evaluatedRecordCount = 0;
      for (Record r : sampleRecords) {
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
        Counter evaluatedRecordCounter =
            MetricsConfigurator.getCounter(metrics, LaneResolver.getPostFixedLaneForObserver(
                lane));
        if (evaluatedRecordCounter == null) {
          evaluatedRecordCounter = MetricsConfigurator.createCounter(metrics, LaneResolver.getPostFixedLaneForObserver(
              lane));
        }
        //counter for the matching records - cummulative sum of records that match criteria
        Counter matchingRecordCounter =
            MetricsConfigurator.getCounter(metrics, USER_PREFIX + dataRuleDefinition.getId());
        if (matchingRecordCounter == null) {
          matchingRecordCounter = MetricsConfigurator.createCounter(metrics, USER_PREFIX + dataRuleDefinition.getId());
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
      return AlertsUtil.evaluateRecord(record, condition, new ELVariables(), EL_EVALUATOR);
    } catch (ObserverException e) {
      //A faulty condition should not take down rest of the alerts with it.
      //Log and it and continue for now
      LOG.error("Error processing metric definition '{}', reason: {}", id, e.getMessage(), e);

      //Trigger alert with exception message
      alertManager.alertException(e.getMessage(), dataRuleDefinition);

      return false;
    }
  }

  @VisibleForTesting
  DataRuleDefinition getDataRuleDefinition() {
    return dataRuleDefinition;
  }

  public static ELEvaluator getElEvaluator() {
    return EL_EVALUATOR;
  }

  public static List<String> getElFunctionIdx() {
    return EL_FUNCTION_IDX;
  }

  public static List<String> getElConstantIdx() {
    return EL_CONSTANT_IDX;
  }
}