/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.google.common.base.Supplier;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.memory.MemoryMonitor;
import com.streamsets.datacollector.memory.MemoryUsageCollector;
import com.streamsets.datacollector.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.HistogramJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;

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
  private Counter memoryConsumedCounter;
  private Meter inputRecordsMeter;
  private Meter outputRecordsMeter;
  private Meter errorRecordsMeter;
  private Meter stageErrorMeter;
  private Histogram inputRecordsHistogram;
  private Histogram outputRecordsHistogram;
  private Histogram errorRecordsHistogram;
  private Histogram stageErrorsHistogram;
  private Map<String, Counter> outputRecordsPerLaneCounter;
  private Map<String, Meter> outputRecordsPerLaneMeter;
  private StagePipe.Context context;
  private final ResourceControlledScheduledExecutor scheduledExecutorService;
  private final MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle;
  private final String name;
  private final String rev;
  private final Configuration configuration;
  private final MetricRegistryJson metricRegistryJson;
  private Map<String, Object> batchMetrics;

  @VisibleForTesting
  StagePipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    this("myPipeline", "0", new Configuration(), stage, inputLanes, outputLanes, new ResourceControlledScheduledExecutor(0.02f),
      new MemoryUsageCollectorResourceBundle(), null);
  }

  public StagePipe(String name, String rev, Configuration configuration, StageRuntime stage, List<String> inputLanes,
                   List<String> outputLanes, ResourceControlledScheduledExecutor scheduledExecutorService,
                   MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle, MetricRegistryJson metricRegistryJson) {
    super(stage, inputLanes, outputLanes);
    this.name = name;
    this.rev = rev;
    this.configuration = configuration;
    this.scheduledExecutorService = scheduledExecutorService;
    this.memoryUsageCollectorResourceBundle = memoryUsageCollectorResourceBundle;
    this.metricRegistryJson = metricRegistryJson;
    this.batchMetrics = new HashMap<>();
  }

  @Override
  public List<Issue> init(StagePipe.Context pipeContext) throws StageException {
    List<Issue> issues = getStage().init();
    if(issues.isEmpty()) {
      MetricRegistry metrics = getStage().getContext().getMetrics();
      String metricsKey = "stage." + getStage().getConfiguration().getInstanceName();
      processingTimer = MetricsConfigurator.createTimer(metrics, metricsKey + ".batchProcessing", name, rev);
      memoryConsumedCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".memoryConsumed", name, rev);
      inputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".inputRecords", name, rev);
      outputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".outputRecords", name, rev);
      errorRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".errorRecords", name, rev);
      stageErrorMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".stageErrors", name, rev);
      inputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".inputRecords", name, rev);
      outputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".outputRecords", name, rev);
      errorRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".errorRecords", name, rev);
      stageErrorsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".stageErrors", name, rev);


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

      if (getStage().getConfiguration().getOutputLanes().size() > 0) {
        outputRecordsPerLaneCounter = new HashMap<>();
        outputRecordsPerLaneMeter = new HashMap<>();
        for (String lane : getStage().getConfiguration().getOutputLanes()) {
          Counter outputRecordsCounter =
            MetricsConfigurator.createCounter(metrics, metricsKey + ":" + lane + ".outputRecords", name, rev);
          if (metricRegistryJson != null) {
            CounterJson counterJson =
              metricRegistryJson.getCounters().get(
                metricsKey + ":" + lane + ".outputRecords" + MetricsConfigurator.COUNTER_SUFFIX);
            outputRecordsCounter.inc(counterJson.getCount());
          }
          outputRecordsPerLaneCounter.put(lane, outputRecordsCounter);

          Meter outputRecordsMeter = MetricsConfigurator.createMeter(
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
      if (configuration.get("monitor.memory", false)) {
        LOG.info("Starting memory collector for {}", getStage().getInfo().getInstanceName());
        scheduledExecutorService.submit(
          new MemoryMonitor(memoryConsumedCounter,
            new Supplier<MemoryUsageCollector>() {
              @Override
              public MemoryUsageCollector get() {
                return new MemoryUsageCollector.Builder()
                  .setMemoryUsageCollectorResourceBundle(memoryUsageCollectorResourceBundle)
                  .setStageRuntime(getStage()).build();
              }
            }));
      }
      createRuntimeStatsGauge(metrics);
    }
    return issues;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    //note down time when this stage was entered
    long startTimeInStage = System.currentTimeMillis();
    //update stats
    updateStatsAtStart(startTimeInStage);

    BatchMakerImpl batchMaker = pipeBatch.startStage(this);
    BatchImpl batchImpl = pipeBatch.getBatch(this);
    ErrorSink errorSink = pipeBatch.getErrorSink();
    String previousOffset = pipeBatch.getPreviousOffset();

    InstanceErrorSink instanceErrorSink = new InstanceErrorSink(getStage().getInfo().getInstanceName(), errorSink);
    FilterRecordBatch.Predicate[] predicates = new FilterRecordBatch.Predicate[2];

    predicates[0] = new RequiredFieldsPredicate(getStage().getRequiredFields());
    predicates[1] = new PreconditionsPredicate(getStage().getContext(), getStage().getPreconditions());

    Batch batch = new FilterRecordBatch(batchImpl, predicates, instanceErrorSink);

    long start = System.currentTimeMillis();
    String newOffset = getStage().execute(previousOffset, pipeBatch.getBatchSize(), batch, batchMaker, errorSink);
    if (isSource()) {
      pipeBatch.setNewOffset(newOffset);
    }

    long processingTime = System.currentTimeMillis() - start;
    processingTimer.update(processingTime, TimeUnit.MILLISECONDS);

    int batchSize = batchImpl.getSize();
    inputRecordsMeter.mark(batchSize);
    inputRecordsHistogram.update(batchSize);

    int stageErrorRecordCount = errorSink.getErrorRecords(getStage().getInfo().getInstanceName()).size();
    errorRecordsMeter.mark(stageErrorRecordCount);
    errorRecordsHistogram.update(stageErrorRecordCount);

    int outputRecordsCount = batchMaker.getSize();
    if (isTarget()) {
      //Assumption is that the target will not drop any record.
      //Records are sent to destination or to the error sink.
      outputRecordsCount = batchSize - stageErrorRecordCount;
    }
    outputRecordsMeter.mark(outputRecordsCount);
    outputRecordsHistogram.update(outputRecordsCount);


    int stageErrorsCount = errorSink.getStageErrors(getStage().getInfo().getInstanceName()).size();
    stageErrorMeter.mark(stageErrorsCount);
    stageErrorsHistogram.update(stageErrorsCount);

    Map<String, Integer> outputRecordsPerLane = new HashMap<>();
    if (getStage().getConfiguration().getOutputLanes().size() > 0) {
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        int outputRecords = batchMaker.getSize(lane);
        outputRecordsPerLane.put(lane, outputRecords);
        outputRecordsPerLaneCounter.get(lane).inc(outputRecords);
        outputRecordsPerLaneMeter.get(lane).mark(outputRecords);
      }
    }

    // capture stage metrics for this batch
    batchMetrics.clear();
    batchMetrics.put(AggregatorUtil.PROCESSING_TIME, processingTime);
    batchMetrics.put(AggregatorUtil.INPUT_RECORDS, batchSize);
    batchMetrics.put(AggregatorUtil.ERROR_RECORDS, stageErrorRecordCount);
    batchMetrics.put(AggregatorUtil.OUTPUT_RECORDS, outputRecordsCount);
    batchMetrics.put(AggregatorUtil.STAGE_ERROR, stageErrorsCount);
    batchMetrics.put(AggregatorUtil.OUTPUT_RECORDS_PER_LANE, outputRecordsPerLane);

    pipeBatch.completeStage(batchMaker);

    //get records count to determine if this stage saw any record in this batch
    int recordsCount = batchSize;
    if(isSource()) {
      //source does not have input records
      recordsCount = outputRecordsCount;
    }
    //update stats
    updateStatsAtEnd(startTimeInStage, newOffset, recordsCount);
  }

  @Override
  public void destroy() {
    getStage().destroy();
  }

  public long getMemoryConsumed() {
    return memoryConsumedCounter.getCount();
  }

  public Map<String, Object> getBatchMetrics() {
    return batchMetrics;
  }

  private Gauge<Object> createRuntimeStatsGauge(MetricRegistry metricRegistry) {
    Gauge<Object> runtimeStatsGauge = MetricsConfigurator.getGauge(metricRegistry, RUNTIME_STATS_GAUGE);
    if(runtimeStatsGauge == null) {
      runtimeStatsGauge = new Gauge<Object>() {
        @Override
        public Object getValue() {
          return context.getRuntimeStats();
        }
      };
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

  private void updateStatsAtStart(long startTimeInStage) {
    //update the runtime stats
    //The following needs to be done at the beginning of a stage per batch
    //1. set name of current stage
    //2. update current batch age, [if source then update the batch age]
    //3. update time in current stage [near zero]
    context.getRuntimeStats().setCurrentStage(getStage().getInfo().getInstanceName());
    //update batch ige if the stage is Source
    if (isSource()) {
      context.getRuntimeStats().setBatchStartTime(System.currentTimeMillis());
    }
    context.getRuntimeStats().setCurrentBatchAge(
      System.currentTimeMillis() - context.getRuntimeStats().getBatchStartTime());
    context.getRuntimeStats().setTimeInCurrentStage(System.currentTimeMillis() - startTimeInStage);
  }

  private void updateStatsAtEnd(long startTimeInStage, String offset, int outputRecordsCount) {
    //update the runtime stats
    //The following needs to be done at the beginning of a stage per batch
    //1. If source, update batch counter, current offset, if there was at least one record in this batch then
    //   update time of last record
    //2. update current batch age
    //3. update time in current stage
    if (isSource()) {
      context.getRuntimeStats().setBatchCount(context.getRuntimeStats().getBatchCount() + 1);
        context.getRuntimeStats().setCurrentSourceOffset(offset);
      if (outputRecordsCount > 0) {
        context.getRuntimeStats().setTimeOfLastReceivedRecord(System.currentTimeMillis());
      }
    }
    context.getRuntimeStats().setCurrentBatchAge(
      System.currentTimeMillis() - context.getRuntimeStats().getBatchStartTime());
    context.getRuntimeStats().setTimeInCurrentStage(System.currentTimeMillis() - startTimeInStage);

  }

  private boolean isSource() {
    if (getStage().getDefinition().getType() == StageType.SOURCE) {
      return true;
    }
    return false;
  }

  private boolean isTarget() {
    if(getStage().getDefinition().getType() == StageType.TARGET) {
      return true;
    }
    return false;
  }

  public interface Context extends Pipe.Context {

    public RuntimeStats getRuntimeStats();

  }
}
