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
package com.streamsets.datacollector.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.event.json.CounterJson;
import com.streamsets.datacollector.event.json.HistogramJson;
import com.streamsets.datacollector.event.json.MeterJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.SourceResponseSink;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StagePipe extends Pipe<StagePipe.Context> {

  private static final Logger LOG = LoggerFactory.getLogger(StagePipe.class);
  //Runtime stat gauge name
  public static final String RUNTIME_STATS_GAUGE = "RuntimeStatsGauge";
  private Timer processingTimer;
  private Meter inputRecordsMeter;
  private Meter outputRecordsMeter;
  private Meter errorRecordsMeter;
  private Meter stageErrorMeter;
  private Counter inputRecordsCounter;
  private Counter outputRecordsCounter;
  private Counter errorRecordsCounter;
  private Counter stageErrorCounter;
  private Histogram inputRecordsHistogram;
  private Histogram outputRecordsHistogram;
  private Histogram errorRecordsHistogram;
  private Histogram stageErrorsHistogram;
  private Map<String, Counter> outputRecordsPerLaneCounter;
  private Map<String, Meter> outputRecordsPerLaneMeter;
  private StagePipe.Context context;
  private final String name;
  private final String rev;
  private final MetricRegistryJson metricRegistryJson;
  private final String stageInstanceName;
  private Map<String, Object> batchMetrics;
  FilterRecordBatch.Predicate[] predicates;

  @VisibleForTesting
  StagePipe(
    StageRuntime stage,
    List<String> inputLanes,
    List<String> outputLanes,
    List<String> eventLanes
  ) {
    this(
      "myPipeline",
      "0",
      stage,
      inputLanes,
      outputLanes,
      eventLanes,
      null
    );
  }

  public StagePipe(
    String name,
    String rev,
    StageRuntime stage,
    List<String> inputLanes,
    List<String> outputLanes,
    List<String> eventLanes,
    MetricRegistryJson metricRegistryJson
  ) {
    super(stage, inputLanes, outputLanes, eventLanes);
    this.name = name;
    this.rev = rev;
    this.metricRegistryJson = metricRegistryJson;
    this.batchMetrics = new HashMap<>();
    this.stageInstanceName = stage.getConfiguration().getInstanceName();
  }

  @Override
  public List<Issue> init(StagePipe.Context pipeContext) throws StageException {
    List<Issue> issues = getStage().init();
    MetricRegistry metrics = getStage().getContext().getMetrics();
    String metricsKey = "stage." + getStage().getConfiguration().getInstanceName();
    processingTimer = MetricsConfigurator.createStageTimer(metrics, metricsKey + ".batchProcessing", name, rev);
    inputRecordsMeter = MetricsConfigurator.createStageMeter(metrics, metricsKey + ".inputRecords", name, rev);
    outputRecordsMeter = MetricsConfigurator.createStageMeter(metrics, metricsKey + ".outputRecords", name, rev);
    errorRecordsMeter = MetricsConfigurator.createStageMeter(metrics, metricsKey + ".errorRecords", name, rev);
    stageErrorMeter = MetricsConfigurator.createStageMeter(metrics, metricsKey + ".stageErrors", name, rev);
    inputRecordsCounter = MetricsConfigurator.createStageCounter(metrics, metricsKey + ".inputRecords", name, rev);
    outputRecordsCounter = MetricsConfigurator.createStageCounter(metrics, metricsKey + ".outputRecords", name, rev);
    errorRecordsCounter = MetricsConfigurator.createStageCounter(metrics, metricsKey + ".errorRecords", name, rev);
    stageErrorCounter = MetricsConfigurator.createStageCounter(metrics, metricsKey + ".stageErrors", name, rev);
    inputRecordsHistogram = MetricsConfigurator.createStageHistogram5Min(metrics, metricsKey + ".inputRecords", name, rev);
    outputRecordsHistogram = MetricsConfigurator.createStageHistogram5Min(metrics, metricsKey + ".outputRecords", name, rev);
    errorRecordsHistogram = MetricsConfigurator.createStageHistogram5Min(metrics, metricsKey + ".errorRecords", name, rev);
    stageErrorsHistogram = MetricsConfigurator.createStageHistogram5Min(metrics, metricsKey + ".stageErrors", name, rev);


    if (metricRegistryJson != null) {
      MeterJson inputRecordsMeterJson =
        metricRegistryJson.getMeters().get(metricsKey + ".inputRecords" + MetricsConfigurator.METER_SUFFIX);
      inputRecordsMeter.mark(inputRecordsMeterJson.getCount());
      MeterJson outputRecordsMeterJson =
        metricRegistryJson.getMeters().get(metricsKey + ".outputRecords" + MetricsConfigurator.METER_SUFFIX);
      outputRecordsMeter.mark(outputRecordsMeterJson.getCount());
      MeterJson errorRecordsMeterJson =
        metricRegistryJson.getMeters().get(metricsKey + ".errorRecords" + MetricsConfigurator.METER_SUFFIX);
      errorRecordsMeter.mark(errorRecordsMeterJson.getCount());
      MeterJson stageErrorMeterJson =
        metricRegistryJson.getMeters().get(metricsKey + ".stageErrors" + MetricsConfigurator.METER_SUFFIX);
      stageErrorMeter.mark(stageErrorMeterJson.getCount());
      CounterJson inputRecordsCounterJson =
        metricRegistryJson.getCounters().get(metricsKey + ".inputRecords" + MetricsConfigurator.COUNTER_SUFFIX);
      inputRecordsCounter.inc(inputRecordsCounterJson.getCount());
      CounterJson outputRecordsCounterJson =
        metricRegistryJson.getCounters().get(metricsKey + ".outputRecords" + MetricsConfigurator.COUNTER_SUFFIX);
      outputRecordsCounter.inc(outputRecordsCounterJson.getCount());
      CounterJson errorRecordsCounterJson =
        metricRegistryJson.getCounters().get(metricsKey + ".errorRecords" + MetricsConfigurator.COUNTER_SUFFIX);
      errorRecordsCounter.inc(errorRecordsCounterJson.getCount());
      CounterJson stageErrorCounterJson =
        metricRegistryJson.getCounters().get(metricsKey + ".stageErrors" + MetricsConfigurator.COUNTER_SUFFIX);
      stageErrorCounter.inc(stageErrorCounterJson.getCount());
      HistogramJson metricHistrogramJson =
        metricRegistryJson.getHistograms()
          .get(metricsKey + ".inputRecords" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
      inputRecordsHistogram.update(metricHistrogramJson.getCount());
      HistogramJson outputRecordsHistogramJson =
        metricRegistryJson.getHistograms().get(
          metricsKey + ".outputRecords" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
      outputRecordsHistogram.update(outputRecordsHistogramJson.getCount());
      HistogramJson errorRecordsHistogramJson =
        metricRegistryJson.getHistograms()
          .get(metricsKey + ".errorRecords" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
      errorRecordsHistogram.update(errorRecordsHistogramJson.getCount());
      HistogramJson stageErrorsHistogramJson =
        metricRegistryJson.getHistograms().get(metricsKey + ".stageErrors" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
      stageErrorsHistogram.update(stageErrorsHistogramJson.getCount());
    }

    if (!getStage().getConfiguration().getOutputAndEventLanes().isEmpty()) {
      outputRecordsPerLaneCounter = new HashMap<>();
      outputRecordsPerLaneMeter = new HashMap<>();
      for (String lane : getStage().getConfiguration().getOutputAndEventLanes()) {
        Counter outputRecordsCounter =
          MetricsConfigurator.createStageCounter(metrics, metricsKey + ":" + lane + ".outputRecords", name, rev);
        if (metricRegistryJson != null) {
          CounterJson counterJson =
            metricRegistryJson.getCounters().get(
              metricsKey + ":" + lane + ".outputRecords" + MetricsConfigurator.COUNTER_SUFFIX);
          outputRecordsCounter.inc(counterJson.getCount());
        }
        outputRecordsPerLaneCounter.put(lane, outputRecordsCounter);

        Meter outputRecordsMeter = MetricsConfigurator.createStageMeter(
          metrics, metricsKey + ":" + lane + ".outputRecords", name, rev);
        if (metricRegistryJson != null) {
          MeterJson meterJson =
            metricRegistryJson.getMeters().get(
              metricsKey + ":" + lane + ".outputRecords" + MetricsConfigurator.METER_SUFFIX);
          outputRecordsMeter.mark(meterJson.getCount());
        }
        outputRecordsPerLaneMeter.put(lane, outputRecordsMeter);
      }
    }
    this.context = pipeContext;
    createRuntimeStatsGauge(metrics);

    predicates = new FilterRecordBatch.Predicate[2];
    predicates[0] = new RequiredFieldsPredicate(getStage().getRequiredFields());
    predicates[1] = new PreconditionsPredicate(getStage().getContext(), getStage().getPreconditions());

    return issues;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException {
    context.getRuntimeStats().addToCurrentStages(this.stageInstanceName);
    BatchMakerImpl batchMaker = pipeBatch.startStage(this);
    BatchImpl batchImpl = pipeBatch.getBatch(this);
    ErrorSink errorSink = pipeBatch.getErrorSink();
    EventSink eventSink = pipeBatch.getEventSink();
    ProcessedSink processedSink = pipeBatch.getProcessedSink();
    SourceResponseSink sourceResponseSink = pipeBatch.getSourceResponseSink();
    String previousOffset = pipeBatch.getPreviousOffset();

    // Filter batch by stage's preconditions
    getStage().setSinks(errorSink, eventSink, processedSink, sourceResponseSink);
    Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());

    long start = System.currentTimeMillis();
    String newOffset = getStage().execute(
        previousOffset,
        pipeBatch.getBatchSize(),
        batch,
        batchMaker,
        errorSink,
        eventSink,
        processedSink,
        sourceResponseSink
    );
    if (isSource()) {
      pipeBatch.setNewOffset(newOffset);
    }

    batchMetrics = finishBatchAndCalculateMetrics(
      start,
      pipeBatch,
      batchMaker,
      batchImpl,
      errorSink,
      eventSink,
      newOffset
    );
  }

  protected Map<String, Object> finishBatchAndCalculateMetrics(
    long startTimeInStage,
    PipeBatch pipeBatch,
    BatchMakerImpl batchMaker,
    BatchImpl batchImpl,
    ErrorSink errorSink,
    EventSink eventSink,
    String newOffset
  ) throws StageException {
    long processingTime = System.currentTimeMillis() - startTimeInStage;
    processingTimer.update(processingTime, TimeUnit.MILLISECONDS);

    int batchSize = batchImpl.getSize();
    inputRecordsCounter.inc(batchSize);
    inputRecordsMeter.mark(batchSize);
    inputRecordsHistogram.update(batchSize);

    int stageErrorRecordCount = errorSink.getErrorRecords(getStage().getInfo().getInstanceName()).size();
    errorRecordsCounter.inc(stageErrorRecordCount);
    errorRecordsMeter.mark(stageErrorRecordCount);
    errorRecordsHistogram.update(stageErrorRecordCount);

    int outputRecordsCount = batchMaker.getSize();
    if (isTargetOrExecutor()) {
      //Assumption is that the target will not drop any record.
      //Records are sent to destination or to the error sink.
      outputRecordsCount = batchSize - stageErrorRecordCount;
    }
    outputRecordsCounter.inc(outputRecordsCount);
    outputRecordsMeter.mark(outputRecordsCount);
    outputRecordsHistogram.update(outputRecordsCount);

    int stageErrorsCount = errorSink.getStageErrors(getStage().getInfo().getInstanceName()).size();
    increaseStageErrorMetrics(stageErrorsCount);

    Map<String, Integer> outputRecordsPerLane = new HashMap<>();
    if (!getStage().getConfiguration().getOutputLanes().isEmpty()) {
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        int outputRecords = batchMaker.getSize(lane);
        outputRecordsPerLane.put(lane, outputRecords);
        outputRecordsPerLaneCounter.get(lane).inc(outputRecords);
        outputRecordsPerLaneMeter.get(lane).mark(outputRecords);
      }
    }

    if(!getStage().getConfiguration().getEventLanes().isEmpty()) {
      String lane = getStage().getConfiguration().getEventLanes().get(0);
      int eventRecords = eventSink.getStageEvents(getStage().getInfo().getInstanceName()).size();
      outputRecordsPerLane.put(lane, eventRecords);
      outputRecordsPerLaneCounter.get(lane).inc(eventRecords);
      outputRecordsPerLaneMeter.get(lane).mark(eventRecords);
    }

    // capture stage metrics for this batch
    Map<String, Object> batchMetrics = new HashMap<>();
    batchMetrics.put(AggregatorUtil.PROCESSING_TIME, processingTime);
    batchMetrics.put(AggregatorUtil.INPUT_RECORDS, batchSize);
    batchMetrics.put(AggregatorUtil.ERROR_RECORDS, stageErrorRecordCount);
    batchMetrics.put(AggregatorUtil.OUTPUT_RECORDS, outputRecordsCount);
    batchMetrics.put(AggregatorUtil.STAGE_ERROR, stageErrorsCount);
    batchMetrics.put(AggregatorUtil.OUTPUT_RECORDS_PER_LANE, outputRecordsPerLane);

    pipeBatch.completeStage(batchMaker);

    // In this is source pipe, update source-specific metrics
    if (isSource()) {
      if (outputRecordsCount > 0) {
        context.getRuntimeStats().setTimeOfLastReceivedRecord(System.currentTimeMillis());
      }
      //Empty batches will increment batch count
      context.getRuntimeStats().incBatchCount();
    }
    context.getRuntimeStats().removeFromCurrentStages(this.stageInstanceName);

    return batchMetrics;
  }

  protected void increaseStageErrorMetrics(int count) {
    stageErrorCounter.inc(count);
    stageErrorMeter.mark(count);
    stageErrorsHistogram.update(count);
  }

  @Override
  public void destroy(PipeBatch pipeBatch) throws StageException {
    EventSink eventSink = pipeBatch.getEventSink();
    ErrorSink errorSink = pipeBatch.getErrorSink();
    ProcessedSink processedSink = pipeBatch.getProcessedSink();

    getStage().destroy(errorSink, eventSink, processedSink);

    pipeBatch.completeStage(this);
  }

  public Map<String, Object> getBatchMetrics() {
    return batchMetrics;
  }

  @SuppressWarnings("unchecked")
  private Gauge<Object> createRuntimeStatsGauge(MetricRegistry metricRegistry) {
    Gauge<Object> runtimeStatsGauge = MetricsConfigurator.getGauge(metricRegistry, RUNTIME_STATS_GAUGE);
    if(runtimeStatsGauge == null) {
      runtimeStatsGauge = () -> context.getRuntimeStats();
      try {
        MetricsConfigurator.createGauge(metricRegistry, RUNTIME_STATS_GAUGE, runtimeStatsGauge, name ,rev);
      } catch (Exception e) {
        for(StackTraceElement se : e.getStackTrace()) {
          LOG.error(se.toString());
        }
        throw e;
      }
    }
    return runtimeStatsGauge;
  }

  private boolean isSource() {
    if (getStage().getDefinition().getType() == StageType.SOURCE) {
      return true;
    }
    return false;
  }

  private boolean isTargetOrExecutor() {
    if(getStage().getDefinition().getType().isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
      return true;
    }
    return false;
  }

  public interface Context extends Pipe.Context {

    RuntimeStats getRuntimeStats();

  }
}
