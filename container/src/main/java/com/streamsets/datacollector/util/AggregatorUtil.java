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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.ThresholdType;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.production.RulesConfigurationChangeRequest;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class AggregatorUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatorUtil.class);

  public static final String METRIC_RULE_RECORD = "MetricRuleRecord";
  public static final String DATA_RULE_RECORD = "DataRuleRecord";
  public static final String CONFIGURATION_CHANGE = "ConfigurationChange";
  public static final String AGGREGATOR = "Aggregator";
  public static final String METRIC_RULE_CHANGE = "MetricRuleChange";
  public static final String DATA_RULE_CHANGE = "DataRuleChange";
  public static final String DATA_RULE_DISABLED = "DataRuleDisabled";
  public static final String METRIC_RULE_DISABLED = "MetricRuleDisabled";
  public static final String STATS_AGGREGATOR_STAGE = "StatsAggregatorStage";
  public static final String METRIC_JSON_STRING = "MetricJsonString";

  public static final String TIMESTAMP = "timestamp";
  public static final String RULE_ID = "ruleId";

  public static final String PIPELINE_BATCH_DURATION = "pipelineBatchDuration";
  public static final String BATCH_COUNT = "batchCount";
  public static final String BATCH_INPUT_RECORDS = "batchInputRecords";
  public static final String BATCH_OUTPUT_RECORDS = "batchOutputRecords";
  public static final String BATCH_ERROR_RECORDS = "batchErrorRecords";
  public static final String BATCH_ERRORS = "batchErrors";
  public static final String STAGE_BATCH_METRICS = "stageBatchMetrics";
  public static final String PROCESSING_TIME = "processingTime";
  public static final String INPUT_RECORDS = "inputRecords";
  public static final String ERROR_RECORDS = "errorRecords";
  public static final String OUTPUT_RECORDS = "outputRecords";
  public static final String STAGE_ERROR = "stageError";
  public static final String OUTPUT_RECORDS_PER_LANE = "outputRecordsPerLane";
  public static final String METRIC_ALERTS_TO_REMOVE = "metricAlertsToRemove";
  public static final String RULES_TO_REMOVE = "rulesToRemove";
  public static final String METRIC_ID = "metricId";
  public static final String ALERT_TEXT = "alertText";
  public static final String CONDITION = "condition";
  public static final String FAMILY = "family";
  public static final String ENABLED = "enabled";
  public static final String SEND_MAIL = "sendMail";
  public static final String EMAILS = "emails";
  public static final String VALID = "valid";
  public static final String METRIC_ELEMENT = "metricElement";
  public static final String METRIC_TYPE = "metricType";

  public static final String LANE = "lane";
  public static final String LABEL = "label";
  public static final String MIN_VOLUME = "minVolume";
  public static final String STREAM_NAME = "streamName";
  public static final String EVALUATED_RECORDS = "evaluatedRecords";
  public static final String MATCHED_RECORDS = "matchedRecords";
  public static final String SAMPLING_PERCENTAGE = "samplingPercentage";
  public static final String SAMPLING_RECORDS_TO_RETAIN = "samplingRecordsToRetain";
  public static final String THRESHOLD_TYPE = "thresholdType";
  public static final String THRESHOLD_VALUE = "thresholdValue";
  public static final String ALERT_ENABLED = "alertEnabled";
  public static final String METER_ENABLED = "meterEnabled";
  public static final String ALERT_TEXTS = "alertTexts";
  public static final String DRIFT_RULE = "driftRule";

  public static final String IS_AGGREGATED = "isAggregated";
  public static final String METADATA = "metadata";
  public static final String SDC_ID = "sdcId";
  public static final String MASTER_SDC_ID = "masterSdcId";
  public static final String TIME_SERIES_ANALYSIS = "timeSeriesAnalysis";
  public static final String LAST_RECORD = "lastRecord";

  private AggregatorUtil() {

  }

  @SuppressWarnings("unchecked")
  public static Record createMetricRecord(Map<String, Object> pipelineBatchMetrics) {
    Record record = createRecord(METRIC_RULE_RECORD);
    Map<String, Field> map = new HashMap<>();
    map.put(TIMESTAMP, Field.create(System.currentTimeMillis()));
    map.put(PIPELINE_BATCH_DURATION, Field.create(Field.Type.LONG, pipelineBatchMetrics.get(PIPELINE_BATCH_DURATION)));
    map.put(BATCH_COUNT, Field.create(Field.Type.INTEGER, pipelineBatchMetrics.get(BATCH_COUNT)));
    map.put(BATCH_INPUT_RECORDS, Field.create(Field.Type.INTEGER, pipelineBatchMetrics.get(BATCH_INPUT_RECORDS)));
    map.put(BATCH_OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, pipelineBatchMetrics.get(BATCH_OUTPUT_RECORDS)));
    map.put(BATCH_ERROR_RECORDS, Field.create(Field.Type.INTEGER, pipelineBatchMetrics.get(BATCH_ERROR_RECORDS)));
    map.put(BATCH_ERRORS, Field.create(Field.Type.INTEGER, pipelineBatchMetrics.get(BATCH_ERRORS)));

    Map<String, Object> stageBatchMetrics = (Map<String, Object>)pipelineBatchMetrics.get(STAGE_BATCH_METRICS);
    // This is stage instance name vs stage metrics

    Map<String, Field> stageMetrics = new HashMap<>();
    for (Map.Entry<String, Object> entry : stageBatchMetrics.entrySet()) {
      Map<String, Field> stageBatchMetricsMap = new HashMap<>();
      Map<String, Object> value = (Map<String, Object>) entry.getValue();
      stageBatchMetricsMap.put(PROCESSING_TIME, Field.create(Field.Type.LONG, value.get(PROCESSING_TIME)));
      stageBatchMetricsMap.put(INPUT_RECORDS, Field.create(Field.Type.INTEGER, value.get(INPUT_RECORDS)));
      stageBatchMetricsMap.put(ERROR_RECORDS, Field.create(Field.Type.INTEGER, value.get(ERROR_RECORDS)));
      stageBatchMetricsMap.put(OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, value.get(OUTPUT_RECORDS)));
      stageBatchMetricsMap.put(STAGE_ERROR, Field.create(Field.Type.INTEGER, value.get(STAGE_ERROR)));

      Map<String, Integer> outputRecordsPerLane = (Map<String, Integer>) value.get(OUTPUT_RECORDS_PER_LANE);
      Map<String, Field> outputRecordsPerLaneMap = new HashMap<>();

      if (outputRecordsPerLane != null) {
        for (Map.Entry<String, Integer> e : outputRecordsPerLane.entrySet()) {
          outputRecordsPerLaneMap.put(e.getKey(), Field.create(e.getValue()));
        }
      }
      stageBatchMetricsMap.put(OUTPUT_RECORDS_PER_LANE, Field.create(outputRecordsPerLaneMap));
      stageMetrics.put(entry.getKey(), Field.create(stageBatchMetricsMap));
    }

    map.put(STAGE_BATCH_METRICS, Field.create(stageMetrics));

    record.set(Field.create(map));
    return record;
  }

  public static Record createDataRuleRecord(
      String ruleId,
      String lane,
      long evaluatedRecordCount,
      long matchingRecordCount,
      List<String> alertTextForMatchRecords,
      boolean driftRule
  ) {
    Record record = createRecord(DATA_RULE_RECORD);
    Map<String, Field> map = new HashMap<>();
    map.put(STREAM_NAME, Field.create(lane));
    map.put(RULE_ID, Field.create(ruleId));
    map.put(TIMESTAMP, Field.create(System.currentTimeMillis()));
    map.put(EVALUATED_RECORDS, Field.create(evaluatedRecordCount));
    map.put(MATCHED_RECORDS, Field.create(matchingRecordCount));
    map.put(DRIFT_RULE, Field.create(driftRule));

    List<Field> alertTextFields = new ArrayList<>(alertTextForMatchRecords.size());
    for (String alertText : alertTextForMatchRecords) {
      alertTextFields.add(Field.create(alertText));
    }
    map.put(ALERT_TEXTS, Field.create(alertTextFields));
    record.set(Field.create(map));
    return record;
  }

  public static Record createConfigChangeRequestRecord(
      RulesConfigurationChangeRequest rulesConfigurationChangeRequest,
      Map<String, Object> resolvedParameters
  ) {
    RuleDefinitionsConfigBean ruleDefinitionsConfigBean = PipelineBeanCreator.get()
        .createRuleDefinitionsConfigBean(
            rulesConfigurationChangeRequest.getRuleDefinitions(),
            new ArrayList<Issue>(),
            resolvedParameters
        );
    List<String> emailIds = ruleDefinitionsConfigBean.emailIDs;
    Record record = createRecord(CONFIGURATION_CHANGE);
    Map<String, Field> map = new HashMap<>();
    map.put(
      METRIC_ALERTS_TO_REMOVE,
        Field.create(
            createListField(
                rulesConfigurationChangeRequest.getMetricAlertsToRemove()
            )
        )
    );
    map.put(
      RULES_TO_REMOVE,
      Field.create(
        createMapField(
          rulesConfigurationChangeRequest.getRulesToRemove()
        )
      )
    );
    map.put(
      EMAILS,
      Field.create(createListField(emailIds))
    );
    map.put(AggregatorUtil.TIMESTAMP, Field.create(System.currentTimeMillis()));
    record.set(Field.create(map));
    return record;
  }

  private static List<Field> createListField(Collection<String> strings) {
    List<Field> fieldList = new ArrayList<>(strings.size());
    for(String s : strings) {
      fieldList.add(Field.create(s));
    }
    return fieldList;
  }

  private static Map<String, Field> createMapField(Map<String, String> map) {
    Map<String, Field> mapField = new HashMap<>();
    for (Map.Entry<String, String> e : map.entrySet()) {
      mapField.put(e.getKey(), Field.create(e.getValue()));
    }
    return mapField;
  }

  private static Record createRecord(String recordSourceId) {
    RecordImpl record = new RecordImpl(AGGREGATOR, recordSourceId, null, null);
    record.addStageToStagePath(STATS_AGGREGATOR_STAGE);
    record.createTrackingId();
    return record;
  }

  public static Record createMetricRuleChangeRecord(MetricsRuleDefinition metricsRuleDefinition) {
    Record record = createRecord(METRIC_RULE_CHANGE);
    Map<String, Field> map = new HashMap<>();
    map.put(RULE_ID, Field.create(metricsRuleDefinition.getId()));
    map.put(METRIC_ID, Field.create(metricsRuleDefinition.getMetricId()));
    map.put(ALERT_TEXT, Field.create(metricsRuleDefinition.getAlertText()));
    map.put(CONDITION, Field.create(metricsRuleDefinition.getCondition()));
    map.put(FAMILY, Field.create(metricsRuleDefinition.getFamily()));
    map.put(TIMESTAMP, Field.create(metricsRuleDefinition.getTimestamp()));
    map.put(ENABLED, Field.create(metricsRuleDefinition.isEnabled()));
    map.put(SEND_MAIL, Field.create(metricsRuleDefinition.isSendEmail()));
    map.put(VALID, Field.create(metricsRuleDefinition.isValid()));
    map.put(METRIC_ELEMENT, Field.create(metricsRuleDefinition.getMetricElement().name()));
    map.put(METRIC_TYPE, Field.create(metricsRuleDefinition.getMetricType().name()));
    record.set(Field.create(map));
    return record;
  }

  public static Record createMetricRuleDisabledRecord(MetricsRuleDefinition metricsRuleDefinition) {
    Record record = createRecord(METRIC_RULE_DISABLED);
    Map<String, Field> map = new HashMap<>();
    map.put(RULE_ID, Field.create(metricsRuleDefinition.getId()));
    map.put(TIMESTAMP, Field.create(metricsRuleDefinition.getTimestamp()));
    record.set(Field.create(map));
    return record;
  }

  public static Record createDataRuleChangeRecord(DataRuleDefinition dataRuleDefinition) {
    Record record = createRecord(DATA_RULE_CHANGE);
    Map<String, Field> map = new HashMap<>();
    map.put(RULE_ID, Field.create(dataRuleDefinition.getId()));
    map.put(ALERT_TEXT, Field.create(dataRuleDefinition.getAlertText()));
    map.put(CONDITION, Field.create(dataRuleDefinition.getCondition()));
    map.put(FAMILY, Field.create(dataRuleDefinition.getFamily()));
    map.put(TIMESTAMP, Field.create(dataRuleDefinition.getTimestamp()));
    map.put(LANE, Field.create(dataRuleDefinition.getLane()));
    map.put(LABEL, Field.create(dataRuleDefinition.getLabel()));
    map.put(MIN_VOLUME, Field.create(dataRuleDefinition.getMinVolume()));
    map.put(SAMPLING_PERCENTAGE, Field.create(dataRuleDefinition.getSamplingPercentage()));
    map.put(SAMPLING_RECORDS_TO_RETAIN, Field.create(dataRuleDefinition.getSamplingRecordsToRetain()));
    map.put(ENABLED, Field.create(dataRuleDefinition.isEnabled()));
    map.put(SEND_MAIL, Field.create(dataRuleDefinition.isSendEmail()));
    map.put(VALID, Field.create(dataRuleDefinition.isValid()));
    map.put(THRESHOLD_TYPE, Field.create(dataRuleDefinition.getThresholdType().name()));
    map.put(THRESHOLD_VALUE, Field.create(dataRuleDefinition.getThresholdValue()));
    map.put(ALERT_ENABLED, Field.create(dataRuleDefinition.isAlertEnabled()));
    map.put(METER_ENABLED, Field.create(dataRuleDefinition.isMeterEnabled()));
    map.put(DRIFT_RULE, Field.create(dataRuleDefinition instanceof DriftRuleDefinition));
    record.set(Field.create(map));
    return record;
  }

  public static Record createDataRuleDisabledRecord(DataRuleDefinition dataRuleDefinition) {
    Record record = createRecord(DATA_RULE_DISABLED);
    Map<String, Field> map = new HashMap<>();
    map.put(RULE_ID, Field.create(dataRuleDefinition.getId()));
    map.put(LANE, Field.create(dataRuleDefinition.getLane()));
    map.put(AggregatorUtil.TIMESTAMP, Field.create(dataRuleDefinition.getTimestamp()));
    record.set(Field.create(map));
    return record;
  }

  public static Record createMetricJsonRecord(
      String sdcId,
      String masterSdcId,
      Map<String, Object> metadata,
      boolean isAggregated,
      boolean timeSeriesAnalysis,
      boolean lastRecord,
      String metricsJSONStr
  ) {
    Record record = createRecord(METRIC_JSON_STRING);
    Map<String, Field> map = new HashMap<>();
    map.put(TIMESTAMP, Field.create(System.currentTimeMillis()));
    map.put(SDC_ID, Field.create(sdcId));
    map.put(MASTER_SDC_ID, Field.create(masterSdcId));
    map.put(IS_AGGREGATED, Field.create(isAggregated));
    map.put(METADATA, getMetadataField(metadata));
    map.put(TIME_SERIES_ANALYSIS, Field.create(timeSeriesAnalysis));
    map.put(LAST_RECORD, Field.create(lastRecord));
    map.put(METRIC_JSON_STRING, Field.create(metricsJSONStr));
    record.set(Field.create(map));
    return record;
  }

  public static MetricsRuleDefinition getMetricRuleDefinition(Record record) {
    return new MetricsRuleDefinition(
        record.get("/" + AggregatorUtil.RULE_ID).getValueAsString(),
        record.get("/" + AggregatorUtil.ALERT_TEXT).getValueAsString(),
        record.get("/" + AggregatorUtil.METRIC_ID).getValueAsString(),
        MetricType.valueOf(record.get("/" + AggregatorUtil.METRIC_TYPE).getValueAsString()),
        MetricElement.valueOf(record.get("/" + AggregatorUtil.METRIC_ELEMENT).getValueAsString()),
        record.get("/" + AggregatorUtil.CONDITION).getValueAsString(),
        record.get("/" + AggregatorUtil.SEND_MAIL).getValueAsBoolean(),
        record.get("/" + AggregatorUtil.ENABLED).getValueAsBoolean(),
        record.get("/" + AggregatorUtil.TIMESTAMP).getValueAsLong()
    );
  }

  public static DataRuleDefinition getDataRuleDefinition(Record record) {
    return new DataRuleDefinition(
      record.get("/" + AggregatorUtil.FAMILY).getValueAsString(),
      record.get("/" + AggregatorUtil.RULE_ID).getValueAsString(),
      record.get("/" + AggregatorUtil.LABEL).getValueAsString(),
      record.get("/" + AggregatorUtil.LANE).getValueAsString(),
      record.get("/" + AggregatorUtil.SAMPLING_PERCENTAGE).getValueAsDouble(),
      record.get("/" + AggregatorUtil.SAMPLING_RECORDS_TO_RETAIN).getValueAsInteger(),
      record.get("/" + AggregatorUtil.CONDITION).getValueAsString(),
      record.get("/" + AggregatorUtil.ALERT_ENABLED).getValueAsBoolean(),
      record.get("/" + AggregatorUtil.ALERT_TEXT).getValueAsString(),
      ThresholdType.valueOf(record.get("/" + AggregatorUtil.THRESHOLD_TYPE).getValueAsString()),
      record.get("/" + AggregatorUtil.THRESHOLD_VALUE).getValueAsString(),
      record.get("/" + AggregatorUtil.MIN_VOLUME).getValueAsLong(),
      record.get("/" + AggregatorUtil.METER_ENABLED).getValueAsBoolean(),
      record.get("/" + AggregatorUtil.SEND_MAIL).getValueAsBoolean(),
      record.get("/" + AggregatorUtil.ENABLED).getValueAsBoolean(),
      record.get("/" + AggregatorUtil.TIMESTAMP).getValueAsLong()
    );
  }

  public static void enqueStatsRecord(
      Record record,
      BlockingQueue<Record> statsQueue, Configuration configuration) {
    boolean offered;
    try {
      offered = statsQueue.offer(
          record,
          configuration.get(
              Constants.MAX_STATS_REQUEST_OFFER_WAIT_TIME_MS_KEY,
              Constants.MAX_STATS_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT
          ),
          TimeUnit.MILLISECONDS
      );
    } catch (InterruptedException e) {
      offered = false;
    }
    if(!offered) {
      LOG.error("Dropping Stats Aggregator Request records as stats aggregator queue is full. " +
        "Please resize the stats aggregator queue.");
    }
  }

  public static Field getMetadataField(Map<String, Object> metadata) {
    if (metadata != null && !metadata.isEmpty()) {
      Map<String, Field> map = new HashMap<>(metadata.size());
      for (Map.Entry<String, Object> e : metadata.entrySet()) {
        if (e.getValue() instanceof String) {
          map.put(e.getKey(), Field.create((String)e.getValue()));
        }
      }
      return Field.create(map);
    }
    return Field.create(Field.Type.MAP, null);
  }

}
