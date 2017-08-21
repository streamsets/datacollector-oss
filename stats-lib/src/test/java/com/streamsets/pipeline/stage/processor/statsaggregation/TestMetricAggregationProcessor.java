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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.io.Resources;
import com.streamsets.datacollector.alerts.AlertsUtil;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class TestMetricAggregationProcessor {

  private MetricAggregationProcessor metricAggregationProcessor;
  private ProcessorRunner runner;

  @Before
  public void setUp() throws IOException, StageException {
    metricAggregationProcessor= getMetricAggregationProcessor();
    runner = new ProcessorRunner.Builder(
      MetricAggregationDProcessor.class,
      metricAggregationProcessor
    ).addOutputLane("a").build();
    runner.runInit();
  }

  @After
  public void tearDown() throws StageException {
    runner.runDestroy();
  }

  @Test
  public void testInit() throws StageException {

    MetricRegistry metrics = metricAggregationProcessor.getMetrics();
    SortedMap<String, Timer> timers = metrics.getTimers();

    Assert.assertEquals(5, timers.size()); // 1 each for 4 stages, 1 for pipeline

    SortedMap<String, Counter> counters = metrics.getCounters();
    Assert.assertEquals(24, counters.size()); // 4 each for 4 stages, 5 for pipeline, one each for 3 output lanes

    SortedMap<String, Meter> meters = metrics.getMeters();
    Assert.assertEquals(24, meters.size()); // 4 each for 4 stages, 5 for pipeline, one each for 3 output lanes

    SortedMap<String, Histogram> histograms = metrics.getHistograms();
    Assert.assertEquals(20, histograms.size()); // 4 each for 4 stages, 4 for pipeline
  }

  @Test
  public void testMetricRuleDisabled() throws StageException {
    long t1 = System.currentTimeMillis();
    long t2 = t1+1;
    MetricsRuleDefinition metricsRuleDefinition1 = TestHelper.createTestMetricRuleDefinition("a < b", true, t1);
    Record metricRuleChangeRecord = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition1);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());

    MetricsRuleDefinition metricsRuleDefinition2 = TestHelper.createTestMetricRuleDefinition("a < b", false, t2);
    metricRuleChangeRecord = AggregatorUtil.createMetricRuleDisabledRecord(metricsRuleDefinition2);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(0, metricAggregationProcessor.getRuleDefinitionMap().size());
  }

  @Test
  public void testDataRuleDisabled() throws StageException {
    DataRuleDefinition dataRuleDefinition1 = TestHelper.createTestDataRuleDefinition("a < b", true, System.currentTimeMillis());
    Record dataRuleChangeRecord = AggregatorUtil.createDataRuleChangeRecord(dataRuleDefinition1);
    runner.runProcess(Arrays.asList(dataRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());

    DataRuleDefinition dataRuleDefinition2 = TestHelper.createTestDataRuleDefinition("a < b", false, System.currentTimeMillis());
    dataRuleChangeRecord = AggregatorUtil.createDataRuleDisabledRecord(dataRuleDefinition2);
    runner.runProcess(Arrays.asList(dataRuleChangeRecord));
    Assert.assertEquals(0, metricAggregationProcessor.getRuleDefinitionMap().size());
  }

  @Test
  public void testMetricRuleChanged() throws StageException {
    long t2 = System.currentTimeMillis();
    long t1 = t2-1;
    long t3 = t2+1;
    MetricsRuleDefinition metricsRuleDefinition1 = TestHelper.createTestMetricRuleDefinition("a < b", true, t2);
    Record metricRuleChangeRecord = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition1);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());
    Assert.assertEquals(
        "a < b",
        metricAggregationProcessor
            .getRuleDefinitionMap()
            .entrySet()
            .iterator()
            .next()
            .getValue()
            .getCondition()
    );

    MetricsRuleDefinition metricsRuleDefinition2 = TestHelper.createTestMetricRuleDefinition("a > b", true, t3);
    metricRuleChangeRecord = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition2);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());
    Assert.assertEquals(
      "a > b",
      metricAggregationProcessor
        .getRuleDefinitionMap()
        .entrySet()
        .iterator()
        .next()
        .getValue()
        .getCondition()
    );

    MetricsRuleDefinition metricsRuleDefinition3 = TestHelper.createTestMetricRuleDefinition("x != y", true, t1);
    metricRuleChangeRecord = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition3);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());
    Assert.assertEquals(
      "a > b",
      metricAggregationProcessor
        .getRuleDefinitionMap()
        .entrySet()
        .iterator()
        .next()
        .getValue()
        .getCondition()
    );
  }

  @Test
  public void testDataRuleChanged() throws StageException {
    long t2 = System.currentTimeMillis();
    long t1 = t2-1;
    long t3 = t2+1;

    DataRuleDefinition dataRuleDefinition1 = TestHelper.createTestDataRuleDefinition("a < b", true, t2);
    Record metricRuleChangeRecord = AggregatorUtil.createDataRuleChangeRecord(dataRuleDefinition1);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());
    Assert.assertEquals(
      "a < b",
      metricAggregationProcessor
        .getRuleDefinitionMap()
        .entrySet()
        .iterator()
        .next()
        .getValue()
        .getCondition()
    );

    DataRuleDefinition dataRuleDefinition2 = TestHelper.createTestDataRuleDefinition("a > b", true, t3);
    metricRuleChangeRecord = AggregatorUtil.createDataRuleChangeRecord(dataRuleDefinition2);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());
    Assert.assertEquals(
      "a > b",
      metricAggregationProcessor
        .getRuleDefinitionMap()
        .entrySet()
        .iterator()
        .next()
        .getValue()
        .getCondition()
    );

    DataRuleDefinition dataRuleDefinition3 = TestHelper.createTestDataRuleDefinition("a > b", true, t1);
    metricRuleChangeRecord = AggregatorUtil.createDataRuleChangeRecord(dataRuleDefinition3);
    runner.runProcess(Arrays.asList(metricRuleChangeRecord));
    Assert.assertEquals(1, metricAggregationProcessor.getRuleDefinitionMap().size());
    Assert.assertEquals(
      "a > b",
      metricAggregationProcessor
        .getRuleDefinitionMap()
        .entrySet()
        .iterator()
        .next()
        .getValue()
        .getCondition()
    );
  }

  @Test
  public void testCountersForMetricRecord() throws StageException {
    MetricsRuleDefinition metricsRuleDefinition1 = TestHelper.createTestMetricRuleDefinition("a < b", true, System.currentTimeMillis());
    Record metricRuleChangeRecord = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition1);
    Record metricRecord = createTestMetricRecord();
    runner.runProcess(Arrays.asList(metricRuleChangeRecord, metricRecord));

    MetricRegistry metrics = metricAggregationProcessor.getMetrics();

    SortedMap<String, Counter> counters = metrics.getCounters();
    Assert.assertTrue(
        counters.containsKey(
            "stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1:com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1OutputLane14578524215930.outputRecords.counter"
        )
    );
    Assert.assertEquals(
        1000,
        counters.get(
          "stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1:com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1OutputLane14578524215930.outputRecords.counter"
        ).getCount()
    );
    Assert.assertTrue(
      counters.containsKey(
        "stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1:com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1OutputLane14578524269500.outputRecords.counter"
      )
    );
    Assert.assertEquals(
      750,
      counters.get(
        "stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1:com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1OutputLane14578524269500.outputRecords.counter"
      ).getCount()
    );
    Assert.assertTrue(
      counters.containsKey(
        "stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1:com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1OutputLane14578524390500.outputRecords.counter"
      )
    );
    Assert.assertEquals(
      500,
      counters.get(
        "stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1:com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1OutputLane14578524390500.outputRecords.counter"
      ).getCount()
    );
  }

  @Test
  public void testMetersForMetricRecord() throws StageException {
    MetricsRuleDefinition metricsRuleDefinition1 = TestHelper.createTestMetricRuleDefinition("a < b", true, System.currentTimeMillis());
    Record metricRuleChangeRecord = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition1);
    Record metricRecord = createTestMetricRecord();
    runner.runProcess(Arrays.asList(metricRuleChangeRecord, metricRecord));

    MetricRegistry metrics = metricAggregationProcessor.getMetrics();

    SortedMap<String, Meter> meters = metrics.getMeters();
    Assert.assertEquals(1, meters.get("pipeline.batchCount.meter").getCount());
    Assert.assertEquals(1000, meters.get("pipeline.batchInputRecords.meter").getCount());
    Assert.assertEquals(500, meters.get("pipeline.batchOutputRecords.meter").getCount());
    Assert.assertEquals(250, meters.get("pipeline.batchErrorRecords.meter").getCount());
    Assert.assertEquals(250, meters.get("pipeline.batchErrorMessages.meter").getCount());

    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1.inputRecords.meter").getCount());
    Assert.assertEquals(1000, meters.get("stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1.outputRecords.meter").getCount());
    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1.errorRecords.meter").getCount());
    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1.stageErrors.meter").getCount());
    Assert.assertEquals(1000, meters.get("stage.com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1:com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1OutputLane14578524215930.outputRecords.meter").getCount());

    Assert.assertEquals(1000, meters.get("stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1.inputRecords.meter").getCount());
    Assert.assertEquals(750, meters.get("stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1.outputRecords.meter").getCount());
    Assert.assertEquals(250, meters.get("stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1.errorRecords.meter").getCount());
    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1.stageErrors.meter").getCount());
    Assert.assertEquals(750, meters.get("stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1:com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1OutputLane14578524269500.outputRecords.meter").getCount());

    Assert.assertEquals(750, meters.get("stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1.inputRecords.meter").getCount());
    Assert.assertEquals(500, meters.get("stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1.outputRecords.meter").getCount());
    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1.errorRecords.meter").getCount());
    Assert.assertEquals(250, meters.get("stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1.stageErrors.meter").getCount());
    Assert.assertEquals(500, meters.get("stage.com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1:com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1OutputLane14578524390500.outputRecords.meter").getCount());

    Assert.assertEquals(500, meters.get("stage.com_streamsets_pipeline_stage_destination_localfilesystem_LocalFileSystemDTarget_1.inputRecords.meter").getCount());
    Assert.assertEquals(500, meters.get("stage.com_streamsets_pipeline_stage_destination_localfilesystem_LocalFileSystemDTarget_1.outputRecords.meter").getCount());
    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_destination_localfilesystem_LocalFileSystemDTarget_1.errorRecords.meter").getCount());
    Assert.assertEquals(0, meters.get("stage.com_streamsets_pipeline_stage_destination_localfilesystem_LocalFileSystemDTarget_1.stageErrors.meter").getCount());

  }

  @Test
  public void testEvaluateDataRuleRecord() throws StageException, InterruptedException {
    Record dataRuleChangeRecord = AggregatorUtil.createDataRuleChangeRecord(
      TestHelper.createTestDataRuleDefinition("a < b", true, System.currentTimeMillis())
    );
    Thread.sleep(2); // Sleep for 2 seconds that the data rule records get a later timestamp
    List<Record> testDataRuleRecords = TestHelper.createTestDataRuleRecords();
    List<Record> batch = new ArrayList<>();
    batch.add(dataRuleChangeRecord);
    batch.addAll(testDataRuleRecords);
    runner.runProcess(batch);
    MetricRegistry metrics = metricAggregationProcessor.getMetrics();

    SortedMap<String, Counter> counters = metrics.getCounters();
    Assert.assertTrue(counters.containsKey("user.x.matched.counter"));
    Assert.assertEquals(400, counters.get("user.x.matched.counter").getCount());

    Assert.assertTrue(counters.containsKey("user.x.evaluated.counter"));
    Assert.assertEquals(2000, counters.get("user.x.evaluated.counter").getCount());

    // Alert expected as threshold type is count and threshold value is 100
    SortedMap<String, Gauge> gauges = metrics.getGauges();
    Assert.assertTrue(gauges.containsKey("alert.x.gauge"));
    Map<String, Object> gaugeValue = (Map<String, Object>) gauges.get("alert.x.gauge").getValue();
    Assert.assertEquals(400L, gaugeValue.get("currentValue"));

    List<String> alertTexts = (List<String>) gaugeValue.get("alertTexts");
    Assert.assertEquals(1, alertTexts.size());
    Assert.assertEquals("Alert!!", alertTexts.get(0));
  }

  @Test
  public void testMetricEvaluation() throws StageException {

    MetricsRuleDefinition metricsRuleDefinition1 = new MetricsRuleDefinition(
      "badRecordsAlertID",
      "High incidence of Bad Records",
      "pipeline.batchErrorRecords.meter",
      MetricType.METER,
      MetricElement.METER_COUNT,
      "${value() > 400}",
      false,
      true,
      System.currentTimeMillis()
    );

    MetricsRuleDefinition metricsRuleDefinition2 = new MetricsRuleDefinition(
      "testMetricAggregation1458001548262",
      "Field Hasher Drops Records",
      "stage.com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1.outputRecords.meter",
      MetricType.METER,
      MetricElement.METER_COUNT,
      "${value() < 1800}",
      false,
      true,
      System.currentTimeMillis()
    );

    Record metricRuleChangeRecord1 = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition1);
    Record metricRuleChangeRecord2 = AggregatorUtil.createMetricRuleChangeRecord(metricsRuleDefinition2);
    Record metricRecord1 = createTestMetricRecord();
    Record metricRecord2 = createTestMetricRecord();
    runner.runProcess(Arrays.asList(metricRuleChangeRecord1, metricRuleChangeRecord2, metricRecord1, metricRecord2));

    // look for alert gauges
    MetricRegistry metrics = metricAggregationProcessor.getMetrics();
    SortedMap<String, Gauge> gauges = metrics.getGauges();
    Assert.assertEquals(2, gauges.size());

    Gauge gauge = gauges.get(AlertsUtil.getAlertGaugeName(metricsRuleDefinition1.getId()) + ".gauge");
    Map<String, Object> alertResponse = (Map<String, Object>) gauge.getValue();
    Assert.assertEquals(500L, alertResponse.get("currentValue"));
    Assert.assertEquals(
        "High incidence of Bad Records",
        ((List<String>)alertResponse.get("alertTexts")).iterator().next()
    );

    gauge = gauges.get(AlertsUtil.getAlertGaugeName(metricsRuleDefinition2.getId()) + ".gauge");
    alertResponse = (Map<String, Object>) gauge.getValue();
    Assert.assertEquals(1500L, alertResponse.get("currentValue"));
    Assert.assertEquals(
        "Field Hasher Drops Records",
        ((List<String>)alertResponse.get("alertTexts")).iterator().next()
    );

  }

  private MetricAggregationProcessor getMetricAggregationProcessor() throws IOException {
    String pipelineConfigJson = Resources.toString(
      Resources.getResource("testMetricAggregation.json"),
      Charset.defaultCharset()
    );
    return new MetricAggregationProcessor(Base64.encodeBase64String(pipelineConfigJson.getBytes()), null,
        "myUrl", null, null, null, "x", 5, 10);
  }

  private Record createTestMetricRecord() {

    Record record = TestHelper.createRecord(AggregatorUtil.METRIC_RULE_RECORD);

    Map<String, Field> map = new HashMap<>();
    map.put(AggregatorUtil.TIMESTAMP, Field.create(System.currentTimeMillis()));

    map.put(AggregatorUtil.PIPELINE_BATCH_DURATION, Field.create(Field.Type.LONG, 4000));
    map.put(AggregatorUtil.BATCH_COUNT, Field.create(Field.Type.INTEGER, 1));
    map.put(AggregatorUtil.BATCH_INPUT_RECORDS, Field.create(Field.Type.INTEGER, 1000));
    map.put(AggregatorUtil.BATCH_OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, 500));
    map.put(AggregatorUtil.BATCH_ERROR_RECORDS, Field.create(Field.Type.INTEGER, 250));
    map.put(AggregatorUtil.BATCH_ERRORS, Field.create(Field.Type.INTEGER, 250));


    // populate per stage metrics
    Map<String, Field> stageMetrics = new HashMap<>();

    Map<String, Field> dirOriginMetrics = new HashMap<>();
    dirOriginMetrics.put(AggregatorUtil.PROCESSING_TIME, Field.create(Field.Type.LONG, 1000));
    dirOriginMetrics.put(AggregatorUtil.INPUT_RECORDS, Field.create(Field.Type.INTEGER, 0));
    dirOriginMetrics.put(AggregatorUtil.ERROR_RECORDS, Field.create(Field.Type.INTEGER, 0));
    dirOriginMetrics.put(AggregatorUtil.OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, 1000));
    dirOriginMetrics.put(AggregatorUtil.STAGE_ERROR, Field.create(Field.Type.INTEGER, 0));
    Map<String, Field> dirOriginOutputPerLane = new HashMap<>();
    dirOriginOutputPerLane.put(
      "com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1OutputLane14578524215930",
      Field.create(1000)
    );
    dirOriginMetrics.put(AggregatorUtil.OUTPUT_RECORDS_PER_LANE, Field.create(dirOriginOutputPerLane));

    Map<String, Field> fieldHashMetrics = new HashMap<>();
    fieldHashMetrics.put(AggregatorUtil.PROCESSING_TIME, Field.create(Field.Type.LONG, 1000));
    fieldHashMetrics.put(AggregatorUtil.INPUT_RECORDS, Field.create(Field.Type.INTEGER, 1000));
    fieldHashMetrics.put(AggregatorUtil.ERROR_RECORDS, Field.create(Field.Type.INTEGER, 250));
    fieldHashMetrics.put(AggregatorUtil.OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, 750));
    fieldHashMetrics.put(AggregatorUtil.STAGE_ERROR, Field.create(Field.Type.INTEGER, 0));
    Map<String, Field> fieldHashOutputPerLane = new HashMap<>();
    fieldHashOutputPerLane.put(
      "com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1OutputLane14578524269500",
      Field.create(750)
    );
    fieldHashMetrics.put(AggregatorUtil.OUTPUT_RECORDS_PER_LANE, Field.create(fieldHashOutputPerLane));

    Map<String, Field> expressionMetrics = new HashMap<>();
    expressionMetrics.put(AggregatorUtil.PROCESSING_TIME, Field.create(Field.Type.LONG, 1000));
    expressionMetrics.put(AggregatorUtil.INPUT_RECORDS, Field.create(Field.Type.INTEGER, 750));
    expressionMetrics.put(AggregatorUtil.ERROR_RECORDS, Field.create(Field.Type.INTEGER, 0));
    expressionMetrics.put(AggregatorUtil.OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, 500));
    expressionMetrics.put(AggregatorUtil.STAGE_ERROR, Field.create(Field.Type.INTEGER, 250));
    Map<String, Field> expressionOutputPerLane = new HashMap<>();
    expressionOutputPerLane.put(
      "com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1OutputLane14578524390500",
      Field.create(500)
    );
    expressionMetrics.put(AggregatorUtil.OUTPUT_RECORDS_PER_LANE, Field.create(expressionOutputPerLane));

    Map<String, Field> localFSMetrics = new HashMap<>();
    localFSMetrics.put(AggregatorUtil.PROCESSING_TIME, Field.create(Field.Type.LONG, 1000));
    localFSMetrics.put(AggregatorUtil.INPUT_RECORDS, Field.create(Field.Type.INTEGER, 500));
    localFSMetrics.put(AggregatorUtil.ERROR_RECORDS, Field.create(Field.Type.INTEGER, 0));
    localFSMetrics.put(AggregatorUtil.OUTPUT_RECORDS, Field.create(Field.Type.INTEGER, 500));
    localFSMetrics.put(AggregatorUtil.STAGE_ERROR, Field.create(Field.Type.INTEGER, 0));

    stageMetrics.put(
      "com_streamsets_pipeline_stage_origin_spooldir_SpoolDirDSource_1",
      Field.create(dirOriginMetrics)
    );
    stageMetrics.put(
      "com_streamsets_pipeline_stage_processor_fieldhasher_FieldHasherDProcessor_1",
      Field.create(fieldHashMetrics)
    );
    stageMetrics.put(
      "com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor_1",
      Field.create(expressionMetrics)
    );
    stageMetrics.put(
      "com_streamsets_pipeline_stage_destination_localfilesystem_LocalFileSystemDTarget_1",
      Field.create(localFSMetrics)
    );

    map.put(AggregatorUtil.STAGE_BATCH_METRICS, Field.create(stageMetrics));

    record.set(Field.create(map));
    return record;
  }



}
