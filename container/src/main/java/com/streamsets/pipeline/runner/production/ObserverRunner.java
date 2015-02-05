/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.alerts.AlertsUtil;
import com.streamsets.pipeline.alerts.MetricAlertsChecker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.MetricsAlertDefinition;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.email.EmailSender;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ObserverException;
import com.streamsets.pipeline.util.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ObserverRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ObserverRunner.class);

  private static final String USER_PREFIX = "user.";
  private static final String CURRENT_VALUE = "currentValue";
  private static final String TIMESTAMP = "timestamp";

  private RulesConfigurationChangeRequest rulesConfigurationChangeRequest;
  private final Map<String, EvictingQueue<Record>> ruleToSampledRecordsMap;
  private final MetricRegistry metrics;
  private final ELEvaluator elEvaluator;
  private final ELEvaluator.Variables variables;
  private final Map<String, Object> alertResponse;
  private final EmailSender emailSender;
  private final Configuration configuration;

  public ObserverRunner(MetricRegistry metrics, EmailSender emailSender, Configuration configuration) {
    this.metrics = metrics;
    this.alertResponse = new HashMap<>();
    this.ruleToSampledRecordsMap = new HashMap<>();
    this.configuration = configuration;
    elEvaluator = new ELEvaluator();
    variables = new ELEvaluator.Variables();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
    this.emailSender = emailSender;
  }

  public void handleObserverRequest(ProductionObserveRequest productionObserveRequest) {

    Map<String, List<Record>> snapshot = productionObserveRequest.getSnapshot();
    for(Map.Entry<String, List<Record>> entry : snapshot.entrySet()) {
      String lane = entry.getKey();
      List<Record> allRecords = entry.getValue();
      List<DataRuleDefinition> dataRuleDefinitions = rulesConfigurationChangeRequest.getLaneToDataRuleMap().get(lane);
      if(dataRuleDefinitions != null) {
        List<Record> sampleRecords = getSampleRecords(dataRuleDefinitions, allRecords);
        for (DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
          if (dataRuleDefinition.isEnabled()) {
            int numberOfRecords = (int) Math.floor(allRecords.size() * dataRuleDefinition.getSamplingPercentage() / 100);

            //cache all sampled records for this data rule definition in an evicting queue
            List<Record> sampleSet = sampleRecords.subList(0, numberOfRecords);
            EvictingQueue<Record> sampledRecords = ruleToSampledRecordsMap.get(dataRuleDefinition.getId());
            if (sampledRecords == null) {
              sampledRecords = EvictingQueue.create(configuration.get(
                com.streamsets.pipeline.prodmanager.Configuration.SAMPLED_RECORDS_CACHE_SIZE_KEY,
                com.streamsets.pipeline.prodmanager.Configuration.SAMPLED_RECORDS_CACHE_SIZE_DEFAULT));
              ruleToSampledRecordsMap.put(dataRuleDefinition.getId(), sampledRecords);
            }
            sampledRecords.addAll(sampleSet);

            //Keep the counters and meters ready before execution
            //batch record counter - cummulative sum of records per batch
            Counter recordCounter = MetricsConfigurator.getCounter(metrics, LaneResolver.getPostFixedLaneForObserver(lane));
            if (recordCounter == null) {
              recordCounter = MetricsConfigurator.createCounter(metrics, LaneResolver.getPostFixedLaneForObserver(lane));
            }
            //counter for the matching records - cummulative sum of records that match criteria
            Counter matchingRecordCounter = MetricsConfigurator.getCounter(metrics, dataRuleDefinition.getId());
            if (matchingRecordCounter == null) {
              matchingRecordCounter = MetricsConfigurator.createCounter(metrics, dataRuleDefinition.getId());
            }
            //Meter
            Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
            if (meter == null) {
              meter = MetricsConfigurator.createMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
            }

            //evaluate sample set of records for condition
            for (Record r : sampleSet) {
              //evaluate
              boolean success = evaluate(r, dataRuleDefinition.getCondition(), dataRuleDefinition.getId());
              //is alert enabled for this rule?
              if (dataRuleDefinition.isAlertEnabled()) {
                recordCounter.inc();
                if (success) {
                  matchingRecordCounter.inc();
                }
                double threshold;
                //Soft error for now as we don't want this alert to stop other rules
                try {
                  threshold = Double.parseDouble(dataRuleDefinition.getThresholdValue());
                } catch (NumberFormatException e) {
                  LOG.error("Error interpreting threshold '{}' as a number", dataRuleDefinition.getThresholdValue());
                  return;
                }
                switch (dataRuleDefinition.getThresholdType()) {
                  case COUNT:
                    if (matchingRecordCounter.getCount() > threshold) {
                      raiseAlert(matchingRecordCounter.getCount(), dataRuleDefinition);
                    }
                    break;
                  case PERCENTAGE:
                    if ((matchingRecordCounter.getCount() * 100 / recordCounter.getCount()) > threshold
                      && recordCounter.getCount() >= dataRuleDefinition.getMinVolume()) {
                      raiseAlert(matchingRecordCounter.getCount(), dataRuleDefinition);
                    }
                    break;
                }
              }
              //is meter enabled?
              if (dataRuleDefinition.isMeterEnabled() && success) {
                meter.mark();
              }
            }
          }
        }
      }
    }
    //pipeline metric alerts
    List<MetricsAlertDefinition> metricsAlertDefinitions =
      rulesConfigurationChangeRequest.getRuleDefinition().getMetricsAlertDefinitions();
    if(metricsAlertDefinitions != null) {
      for (MetricsAlertDefinition metricsAlertDefinition : metricsAlertDefinitions) {
        MetricAlertsChecker metricAlertsHelper = new MetricAlertsChecker(metricsAlertDefinition, metrics,
          variables, elEvaluator, emailSender);
        metricAlertsHelper.checkForAlerts();
      }
    }
  }

  public void handleConfigurationChangeRequest(RulesConfigurationChangeRequest rulesConfigurationChangeRequest) {
    //update config changes
    this.rulesConfigurationChangeRequest = rulesConfigurationChangeRequest;

    //remove metrics for changed / deleted rules
    for(String ruleId : rulesConfigurationChangeRequest.getRulesToRemove()) {
      MetricsConfigurator.removeMeter(metrics, ruleId);
      MetricsConfigurator.removeCounter(metrics, ruleId);
    }
    for(String alertId : rulesConfigurationChangeRequest.getMetricAlertsToRemove()) {
      MetricsConfigurator.removeGauge(metrics, alertId);
    }
  }

  private boolean evaluate(Record record, String condition, String id) {
    try {
      return AlertsUtil.evaluateRecord(record, condition, variables, elEvaluator);
    } catch (ObserverException e) {
      //A faulty condition should not take down rest of the alerts with it.
      //Log and it and continue for now
      LOG.error("Error processing metric definition '{}', reason: {}", id, e.getMessage());
      return false;
    }
  }

  private void raiseAlert(Object value, DataRuleDefinition dataRuleDefinition) {
    alertResponse.put(CURRENT_VALUE, value);
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    if (gauge == null) {
      alertResponse.put(TIMESTAMP, System.currentTimeMillis());
      //send email the first time alert is triggered
      if(dataRuleDefinition.isSendEmail()) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(CURRENT_VALUE + " = " + value).append(", " + TIMESTAMP + " = " +
          alertResponse.get(TIMESTAMP));
        if(emailSender == null) {
          LOG.warn("Email Sender is not configured. Alert '{}' with message '{}' will not be sent via email.",
            dataRuleDefinition.getId(), stringBuilder.toString());
        } else {
          try {
            emailSender.send(dataRuleDefinition.getEmailIds(), dataRuleDefinition.getId(), stringBuilder.toString());
          } catch (PipelineException e) {
            LOG.error("Error sending alert email, reason: {}", e.getMessage());
            //Log error and move on. This should not stop the pipeline.
          }
        }
      }
    } else {
      //remove existing gauge
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
      alertResponse.put(TIMESTAMP, ((Map<String, Object>)gauge.getValue()).get(TIMESTAMP));
    }
    Gauge<Object> alertResponseGauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return alertResponse;
      }
    };
    MetricsConfigurator.createGuage(metrics, AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()),
      alertResponseGauge);
  }

  private List<Record> getSampleRecords(List<DataRuleDefinition> dataRuleDefinitions, List<Record> allRecords) {
    double percentage = 0;
    for(DataRuleDefinition dataRuleDefinition : dataRuleDefinitions) {
      if(dataRuleDefinition.getSamplingPercentage() > percentage) {
        percentage = dataRuleDefinition.getSamplingPercentage();
      }
    }
    Collections.shuffle(allRecords);
    double numberOfRecordsToSample = Math.floor(allRecords.size() * percentage / 100);
    List<Record> sampleRecords = new ArrayList<>((int)numberOfRecordsToSample);
    for(int i = 0; i < numberOfRecordsToSample; i++) {
      sampleRecords.add(((RecordImpl) allRecords.get(i)).clone());
    }
    return sampleRecords;
  }

  public List<Record> getSampledRecords(String ruleId, int size) {
    if(ruleToSampledRecordsMap.get(ruleId) != null) {
      if(ruleToSampledRecordsMap.get(ruleId).size() > size) {
        return new CopyOnWriteArrayList<>(ruleToSampledRecordsMap.get(ruleId)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(ruleToSampledRecordsMap.get(ruleId));
      }
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  RulesConfigurationChangeRequest getRulesConfigurationChangeRequest() {
    return this.rulesConfigurationChangeRequest;
  }

}
