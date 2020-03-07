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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.execution.alerts.AlertManagerHelper;
import com.streamsets.datacollector.execution.alerts.MetricRuleEvaluatorHelper;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.ObserverException;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricRuleHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricRuleHandler.class);

  // each stage within a pipeline has the following metrics.
  // The below maps maintain the stage instance name to metric mapping
  private Map<String, Timer> stageProcessingTimer;
  private Map<String, Counter> inputRecordsCounter;
  private Map<String, Counter> outputRecordsCounter;
  private Map<String, Counter> errorRecordsCounter;
  private Map<String, Counter> stageErrorCounter;
  private Map<String, Meter> inputRecordsMeter;
  private Map<String, Meter> outputRecordsMeter;
  private Map<String, Meter> errorRecordsMeter;
  private Map<String, Meter> stageErrorMeter;
  private Map<String, Histogram> inputRecordsHistogram;
  private Map<String, Histogram> outputRecordsHistogram;
  private Map<String, Histogram> errorRecordsHistogram;
  private Map<String, Histogram> stageErrorsHistogram;
  private Map<String, Map<String, Counter>> outputRecordsPerLaneCounter;
  private Map<String, Map<String, Meter>> outputRecordsPerLaneMeter;

  // pipeline batch timer, meter and histogram
  private Timer batchProcessingTimer;
  private Counter batchCountCounter;
  private Meter batchCountMeter;
  private Histogram batchInputRecordsHistogram;
  private Histogram batchOutputRecordsHistogram;
  private Histogram batchErrorRecordsHistogram;
  private Histogram batchErrorsHistogram;
  private Meter batchInputRecordsMeter;
  private Meter batchOutputRecordsMeter;
  private Meter batchErrorRecordsMeter;
  private Meter batchErrorMessagesMeter;
  private Counter batchInputRecordsCounter;
  private Counter batchOutputRecordsCounter;
  private Counter batchErrorRecordsCounter;
  private Counter batchErrorMessagesCounter;

  private final String pipelineName;
  private final String revision;
  private final Map<String, RuleDefinition> ruleDefinitionMap;
  private final MetricRegistry metrics;
  private final RulesEvaluator evaluator;

  public MetricRuleHandler(
    Stage.Context context,
    String pipelineName,
    String revision,
    String pipelineUrl,
    MetricRegistry metrics,
    MetricRegistryJson metricRegistryJson,
    PipelineConfiguration pipelineConfiguration,
    Map<String, RuleDefinition> ruleDefinitionMap
  ) {
    this.pipelineName = pipelineName;
    this.revision = revision;
    this.metrics = metrics;
    this.ruleDefinitionMap = ruleDefinitionMap;
    this.evaluator = new RulesEvaluator(pipelineName, revision, pipelineUrl, context);
    initializeMetricRegistry(metrics, metricRegistryJson, pipelineConfiguration);
  }

  void handleMetricRuleRecord(Record record) {
    // update all the counters, meters, histograms from this record
    batchProcessingTimer.update(
      record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.PIPELINE_BATCH_DURATION).getValueAsLong(),
      TimeUnit.MILLISECONDS
    );
    batchCountCounter.inc();
    batchCountMeter.mark();

    int batchInputRecords = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.BATCH_INPUT_RECORDS).getValueAsInteger();
    batchInputRecordsHistogram.update(batchInputRecords);
    batchInputRecordsMeter.mark(batchInputRecords);
    batchInputRecordsCounter.inc(batchInputRecords);

    int batchOutputRecords = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.BATCH_OUTPUT_RECORDS).getValueAsInteger();
    batchOutputRecordsHistogram.update(batchOutputRecords);
    batchOutputRecordsMeter.mark(batchOutputRecords);
    batchOutputRecordsCounter.inc(batchOutputRecords);

    int batchErrorRecords = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.BATCH_ERROR_RECORDS).getValueAsInteger();
    batchErrorRecordsHistogram.update(batchErrorRecords);
    batchErrorRecordsMeter.mark(batchErrorRecords);
    batchErrorRecordsCounter.inc(batchErrorRecords);

    int batchErrors = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.BATCH_ERRORS).getValueAsInteger();
    batchErrorsHistogram.update(batchErrors);
    batchErrorMessagesMeter.mark(batchErrors);
    batchErrorMessagesCounter.inc(batchErrors);

    Map<String, Field> stageBatchMetrics = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.STAGE_BATCH_METRICS)
      .getValueAsListMap();

    // for every instance of the stage fetch the stats and update it
    for (Map.Entry<String, Field> entry : stageBatchMetrics.entrySet()) {
      Map<String, Field> stageMetrics = entry.getValue().getValueAsListMap();

      String stageInstanceName = entry.getKey();

      // stats values can be null, guard against it
      Field field = stageMetrics.get(AggregatorUtil.PROCESSING_TIME);
      if (field.getValue() != null) {
        stageProcessingTimer.get(stageInstanceName).update(field.getValueAsLong(), TimeUnit.MILLISECONDS);
      }

      field = stageMetrics.get(AggregatorUtil.INPUT_RECORDS);
      if (field.getValue() != null) {
        int inputRecords = field.getValueAsInteger();
        inputRecordsCounter.get(stageInstanceName).inc(inputRecords);
        inputRecordsMeter.get(stageInstanceName).mark(inputRecords);
        inputRecordsHistogram.get(stageInstanceName).update(inputRecords);
      }

      field = stageMetrics.get(AggregatorUtil.OUTPUT_RECORDS);
      if (field.getValue() != null) {
        int outputRecords = field.getValueAsInteger();
        outputRecordsCounter.get(stageInstanceName).inc(outputRecords);
        outputRecordsMeter.get(stageInstanceName).mark(outputRecords);
        outputRecordsHistogram.get(stageInstanceName).update(outputRecords);
      }

      field = stageMetrics.get(AggregatorUtil.ERROR_RECORDS);
      if (field.getValue() != null) {
        int errorRecords = field.getValueAsInteger();
        errorRecordsCounter.get(stageInstanceName).inc(errorRecords);
        errorRecordsMeter.get(stageInstanceName).mark(errorRecords);
        errorRecordsHistogram.get(stageInstanceName).update(errorRecords);
      }


      field = stageMetrics.get(AggregatorUtil.STAGE_ERROR);
      if (field.getValue() != null) {
        int stageErrors = field.getValueAsInteger();
        stageErrorCounter.get(stageInstanceName).inc(stageErrors);
        stageErrorMeter.get(stageInstanceName).mark(stageErrors);
        stageErrorsHistogram.get(stageInstanceName).update(stageErrors);
      }

      // no output lanes present for destinations
      Map<String, Counter> stringCounterMap = outputRecordsPerLaneCounter.get(stageInstanceName);
      Map<String, Meter> stringMeterMap = outputRecordsPerLaneMeter.get(stageInstanceName);
      if (stageMetrics.containsKey(AggregatorUtil.OUTPUT_RECORDS_PER_LANE)) {
        // destination stage does not have output records per lane
        Map<String, Field> valueAsMap = stageMetrics.get(AggregatorUtil.OUTPUT_RECORDS_PER_LANE)
          .getValueAsMap();
        if (valueAsMap != null && !stringCounterMap.isEmpty()) {
          for (Map.Entry<String, Field> e : valueAsMap.entrySet()) {
            if (e.getValue().getValue() != null) {
              int recordCount = e.getValue().getValueAsInteger();
              stringCounterMap.get(e.getKey()).inc(recordCount);
              stringMeterMap.get(e.getKey()).mark(recordCount);
            }
          }
        }
      }
    }
  }

  void handleConfigChangeRecord(Record record) {

    List<Field> valueAsList = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.EMAILS).getValueAsList();
    List<String> emails = new ArrayList<>();
    for (Field f : valueAsList) {
      emails.add(f.getValueAsString());
    }
    evaluator.setEmails(emails);

    Map<String, Field> rulesToRemove = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULES_TO_REMOVE).getValueAsListMap();
    for (Map.Entry<String, Field> e : rulesToRemove.entrySet()) {
      String ruleId = e.getKey();
      MetricsConfigurator.removeMeter(metrics, MetricAggregationConstants.USER_PREFIX + ruleId, pipelineName, revision);
      MetricsConfigurator.removeCounter(metrics, MetricAggregationConstants.USER_PREFIX + ruleId, pipelineName, revision);
    }

    List<Field> alertsToRemove = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.METRIC_ALERTS_TO_REMOVE).getValueAsList();
    for (Field f : alertsToRemove) {
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(f.getValueAsString()), pipelineName, revision);
    }

  }

  void handleRuleDisabledRecord(Record record) {
    if (RulesHelper.isRuleDefinitionLatest(ruleDefinitionMap, record)) {
      // remove alertDataRule gauge if present
      String ruleId = record.get(MetricAggregationConstants.ROOT_FIELD + AggregatorUtil.RULE_ID).getValueAsString();
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(ruleId), pipelineName, revision);
      ruleDefinitionMap.remove(ruleId);
    }
  }

  void handleRuleChangeRecord(Record record) {
    if (RulesHelper.isRuleDefinitionLatest(ruleDefinitionMap, record)) {
      RuleDefinition rDef = AggregatorUtil.getMetricRuleDefinition(record);
      MetricsConfigurator.removeGauge(metrics, AlertsUtil.getAlertGaugeName(rDef.getId()), pipelineName, revision);
      ruleDefinitionMap.put(rDef.getId(), rDef);
    }
  }

  void evaluateMetricRules(MetricsRuleDefinition metricsRuleDefinition) {
    if (metricsRuleDefinition.isEnabled()) {
      Object value = null;
      try {
        Metric metric = MetricRuleEvaluatorHelper.getMetric(metrics,
            metricsRuleDefinition.getMetricId(),
            metricsRuleDefinition.getMetricType()
        );
        if (metric != null) {
          value = MetricRuleEvaluatorHelper.getMetricValue(metricsRuleDefinition.getMetricElement(),
              metricsRuleDefinition.getMetricType(),
              metric
          );

          if (value != null) {
            // We get start time from PipelineEL directly
            if (MetricRuleEvaluatorHelper.evaluate(
                PipelineEL.startTime().getTime(),
                value,
                metricsRuleDefinition.getCondition()
            )) {
              evaluator.raiseAlert(metrics, metricsRuleDefinition, value);
            }
          }
        }
      } catch (ObserverException ex) {
        LOG.error(
            "Error processing metric definition alertDataRule '{}', reason: {}",
            metricsRuleDefinition.getId(),
            ex.toString(),
            ex
        );
        AlertManagerHelper.alertException(pipelineName, revision, metrics, value, metricsRuleDefinition);
      }
    }
  }

  private void initializeMetricRegistry(
    MetricRegistry metrics,
    MetricRegistryJson metricRegistryJson,
    PipelineConfiguration pipelineConfiguration
  ) {

    batchProcessingTimer = MetricsHelper.createAndInitTimer(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_PROCESSING,
      pipelineName,
      revision
    );
    batchCountCounter = MetricsHelper.createAndInitCounter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_COUNT,
      pipelineName,
      revision
    );
    batchCountMeter = MetricsHelper.createAndInitMeter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_COUNT,
      pipelineName,
      revision
    );
    batchInputRecordsHistogram = MetricsHelper.createAndInitHistogram(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_INPUT_RECORDS_PER_BATCH,
      pipelineName,
      revision
    );
    batchOutputRecordsHistogram = MetricsHelper.createAndInitHistogram(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_OUTPUT_RECORDS_PER_BATCH,
      pipelineName,
      revision
    );
    batchErrorRecordsHistogram = MetricsHelper.createAndInitHistogram(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_ERROR_RECORDS_PER_BATCH,
      pipelineName,
      revision
    );
    batchErrorsHistogram = MetricsHelper.createAndInitHistogram(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_ERRORS_PER_BATCH,
      pipelineName,
      revision
    );
    batchInputRecordsMeter = MetricsHelper.createAndInitMeter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_INPUT_RECORDS,
      pipelineName,
      revision
    );
    batchOutputRecordsMeter = MetricsHelper.createAndInitMeter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_OUTPUT_RECORDS,
      pipelineName,
      revision
    );
    batchErrorRecordsMeter = MetricsHelper.createAndInitMeter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_ERROR_RECORDS,
      pipelineName,
      revision
    );
    batchErrorMessagesMeter = MetricsHelper.createAndInitMeter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_ERROR_MESSAGES,
      pipelineName,
      revision
    );
    batchInputRecordsCounter = MetricsHelper.createAndInitCounter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_INPUT_RECORDS,
      pipelineName,
      revision
    );
    batchOutputRecordsCounter = MetricsHelper.createAndInitCounter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_OUTPUT_RECORDS,
      pipelineName,
      revision
    );
    batchErrorRecordsCounter = MetricsHelper.createAndInitCounter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_ERROR_RECORDS,
      pipelineName,
      revision
    );
    batchErrorMessagesCounter = MetricsHelper.createAndInitCounter(
      metricRegistryJson,
      metrics,
      MetricAggregationConstants.PIPELINE_BATCH_ERROR_MESSAGES,
      pipelineName,
      revision
    );

    // create metric for stages
    stageProcessingTimer = new HashMap<>();
    inputRecordsCounter = new HashMap<>();
    outputRecordsCounter = new HashMap<>();
    errorRecordsCounter = new HashMap<>();
    stageErrorCounter = new HashMap<>();
    inputRecordsMeter = new HashMap<>();
    outputRecordsMeter = new HashMap<>();
    errorRecordsMeter = new HashMap<>();
    stageErrorMeter = new HashMap<>();
    inputRecordsHistogram = new HashMap<>();
    outputRecordsHistogram = new HashMap<>();
    errorRecordsHistogram = new HashMap<>();
    stageErrorsHistogram = new HashMap<>();
    outputRecordsPerLaneCounter = new HashMap<>();
    outputRecordsPerLaneMeter = new HashMap<>();

    for (StageConfiguration s : pipelineConfiguration.getStages()) {
      String stageInstanceName = s.getInstanceName();
      String metricsKey = MetricAggregationConstants.STAGE_PREFIX + stageInstanceName;
      stageProcessingTimer.put(
        stageInstanceName,
        MetricsHelper.createAndInitTimer(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.BATCH_PROCESSING,
          pipelineName,
          revision
        )
      );
      inputRecordsCounter.put(
        stageInstanceName,
        MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.INPUT_RECORDS,
          pipelineName,
          revision
        )
      );
      outputRecordsCounter.put(
        stageInstanceName,
        MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.OUTPUT_RECORDS,
          pipelineName,
          revision
        )
      );
      errorRecordsCounter.put(
        stageInstanceName,
        MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.ERROR_RECORDS,
          pipelineName,
          revision
        )
      );
      stageErrorCounter.put(
        stageInstanceName,
        MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.STAGE_ERRORS,
          pipelineName,
          revision
        )
      );
      inputRecordsMeter.put(
        stageInstanceName,
        MetricsHelper.createAndInitMeter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.INPUT_RECORDS,
          pipelineName,
          revision
        )
      );
      outputRecordsMeter.put(
        stageInstanceName,
        MetricsHelper.createAndInitMeter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.OUTPUT_RECORDS,
          pipelineName,
          revision
        )
      );
      errorRecordsMeter.put(
        stageInstanceName,
        MetricsHelper.createAndInitMeter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.ERROR_RECORDS,
          pipelineName,
          revision
        )
      );
      stageErrorMeter.put(
        stageInstanceName,
        MetricsHelper.createAndInitMeter(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.STAGE_ERRORS,
          pipelineName,
          revision
        )
      );
      inputRecordsHistogram.put(
        stageInstanceName,
        MetricsHelper.createAndInitHistogram(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.INPUT_RECORDS,
          pipelineName,
          revision
        )
      );
      outputRecordsHistogram.put(
        stageInstanceName,
        MetricsHelper.createAndInitHistogram(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.OUTPUT_RECORDS,
          pipelineName,
          revision
        )
      );
      errorRecordsHistogram.put(
        stageInstanceName,
        MetricsHelper.createAndInitHistogram(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.ERROR_RECORDS,
          pipelineName,
          revision
        )
      );
      stageErrorsHistogram.put(
        stageInstanceName,
        MetricsHelper.createAndInitHistogram(
          metricRegistryJson,
          metrics,
          metricsKey + MetricAggregationConstants.STAGE_ERRORS,
          pipelineName,
          revision
        )
      );

      // create counter and meter for output & event lanes
      Map<String, Meter> outputLaneToMeterMap = new HashMap<>();
      Map<String, Counter> outputLaneToCounterMap = new HashMap<>();
      for (String lane : s.getOutputAndEventLanes()) {
        outputLaneToMeterMap.put(
          lane,
          MetricsHelper.createAndInitMeter(
            metricRegistryJson,
            metrics,
            metricsKey + ":" + lane + MetricAggregationConstants.OUTPUT_RECORDS,
            pipelineName,
            revision
          )
        );
        Counter counter = MetricsHelper.createAndInitCounter(
          metricRegistryJson,
          metrics,
          metricsKey + ":" + lane + MetricAggregationConstants.OUTPUT_RECORDS,
          pipelineName,
          revision
        );
        outputLaneToCounterMap.put(lane, counter);
      }
      outputRecordsPerLaneMeter.put(stageInstanceName, outputLaneToMeterMap);
      outputRecordsPerLaneCounter.put(stageInstanceName, outputLaneToCounterMap);
    }
  }
}
