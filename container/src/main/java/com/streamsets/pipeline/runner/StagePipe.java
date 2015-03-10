/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.validation.StageIssue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StagePipe extends Pipe {

  //static variables needed to update the runtime stat gauge
  private static final String RUNTIME_STATS_GAUGE = "RuntimeStatsGauge";
  private static final RuntimeStats RUNTIME_STATS = new RuntimeStats();
  private static int BATCH_COUNTER = 0;
  private static long BATCH_AGE = 0;
  private static long TIME_OF_LAST_RECORD = 0;

  private Timer processingTimer;
  private Counter inputRecordsCounter;
  private Counter outputRecordsCounter;
  private Counter errorRecordsCounter;
  private Counter stageErrorCounter;
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

  public StagePipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    super(stage, inputLanes, outputLanes);
  }

  @Override
  public List<StageIssue> validateConfigs() throws StageException {
    return getStage().validateConfigs();
  }

  @Override
  public void init() throws StageException {
    getStage().init();
    MetricRegistry metrics = getStage().getContext().getMetrics();
    String metricsKey = "stage." + getStage().getConfiguration().getInstanceName();
    processingTimer = MetricsConfigurator.createTimer(metrics, metricsKey + ".batchProcessing");
    inputRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".inputRecords");
    outputRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".outputRecords");
    errorRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".errorRecords");
    stageErrorCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".stageErrors");
    inputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".inputRecords");
    outputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".outputRecords");
    errorRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".errorRecords");
    stageErrorMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".stageErrors");
    inputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".inputRecords");
    outputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".outputRecords");
    errorRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".errorRecords");
    stageErrorsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".stageErrors");
    if (getStage().getConfiguration().getOutputLanes().size() > 1) {
      outputRecordsPerLaneCounter = new HashMap<>();
      outputRecordsPerLaneMeter = new HashMap<>();
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        outputRecordsPerLaneCounter.put(lane, MetricsConfigurator.createCounter(
          metrics, metricsKey + ":" + lane + ".outputRecords"));
        outputRecordsPerLaneMeter.put(lane, MetricsConfigurator.createMeter(
          metrics, metricsKey + ":" + lane + ".outputRecords"));
      }
    }
    createRuntimeStatsGauge(metrics);
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

    RequiredFieldsErrorPredicateSink predicateSink = new RequiredFieldsErrorPredicateSink(
        getStage().getInfo().getInstanceName(), getStage().getRequiredFields(), errorSink);
    Batch batch = new FilterRecordBatch(batchImpl, predicateSink, predicateSink);

    long start = System.currentTimeMillis();
    String newOffset = getStage().execute(previousOffset, pipeBatch.getBatchSize(), batch, batchMaker,
                                          errorSink);
    if (isSource()) {
      pipeBatch.setNewOffset(newOffset);
    }
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);

    inputRecordsCounter.inc(batchImpl.getSize());
    inputRecordsMeter.mark(batchImpl.getSize());
    inputRecordsHistogram.update(batchImpl.getSize());

    int stageErrorRecordCount = errorSink.getErrorRecords(getStage().getInfo().getInstanceName()).size();
    errorRecordsCounter.inc(stageErrorRecordCount);
    errorRecordsMeter.mark(stageErrorRecordCount);
    errorRecordsHistogram.update(stageErrorRecordCount);

    int outputRecordsCount = batchMaker.getSize();
    if (isTarget()) {
      //Assumption is that the target will not drop any record.
      //Records are sent to destination or to the error sink.
      outputRecordsCount = batchImpl.getSize() - stageErrorRecordCount;
    }
    outputRecordsCounter.inc(outputRecordsCount);
    outputRecordsMeter.mark(outputRecordsCount);
    outputRecordsHistogram.update(outputRecordsCount);


    int stageErrorsCount = errorSink.getStageErrors(getStage().getInfo().getInstanceName()).size();
    stageErrorCounter.inc(stageErrorsCount);
    stageErrorMeter.mark(stageErrorsCount);
    stageErrorsHistogram.update(stageErrorsCount);

    if (getStage().getConfiguration().getOutputLanes().size() > 1) {
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        outputRecordsPerLaneCounter.get(lane).inc(batchMaker.getSize(lane));
        outputRecordsPerLaneMeter.get(lane).mark(batchMaker.getSize(lane));
      }
    }
    pipeBatch.completeStage(batchMaker);

    //get records count to determine if this stage saw any record in this batch
    int recordsCount = batchImpl.getSize();
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

  private Gauge<Object> createRuntimeStatsGauge(MetricRegistry metricRegistry) {
    Gauge<Object> runtimeStatsGauge = MetricsConfigurator.getGauge(metricRegistry, RUNTIME_STATS_GAUGE);
    if(runtimeStatsGauge == null) {
      runtimeStatsGauge = new Gauge<Object>() {
        @Override
        public Object getValue() {
          return RUNTIME_STATS;
        }
      };
      MetricsConfigurator.createGauge(metricRegistry, RUNTIME_STATS_GAUGE, runtimeStatsGauge);
    }
    return runtimeStatsGauge;
  }

  private void updateStatsAtStart(long startTimeInStage) {
    //update the runtime stats
    //The following needs to be done at the beginning of a stage per batch
    //1. set name of current stage
    //2. update current batch age, [if source then update the batch age]
    //3. update time in current stage [near zero]
    RUNTIME_STATS.setCurrentStage(getStage().getInfo().getInstanceName());
    //update batch ige if the stage is Source
    if (isSource()) {
      BATCH_AGE = System.currentTimeMillis();
    }
    RUNTIME_STATS.setCurrentBatchAge(System.currentTimeMillis() - BATCH_AGE);
    RUNTIME_STATS.setTimeInCurrentStage(System.currentTimeMillis() - startTimeInStage);
  }

  private void updateStatsAtEnd(long startTimeInStage, String offset, int outputRecordsCount) {
    //update the runtime stats
    //The following needs to be done at the beginning of a stage per batch
    //1. If source, update batch counter, current offset, if there was at least one record in this batch then
    //   update time of last record
    //2. update current batch age
    //3. update time in current stage
    if (isSource()) {
      BATCH_COUNTER++;
      RUNTIME_STATS.setCurrentSourceOffset(offset);
      if (outputRecordsCount > 0) {
        TIME_OF_LAST_RECORD = System.currentTimeMillis();
      }
    }
    RUNTIME_STATS.setBatchCount(BATCH_COUNTER);
    RUNTIME_STATS.setCurrentBatchAge(System.currentTimeMillis() - BATCH_AGE);
    RUNTIME_STATS.setTimeInCurrentStage(System.currentTimeMillis() - startTimeInStage);
    RUNTIME_STATS.setTimeOfLastReceivedRecord(TIME_OF_LAST_RECORD);
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

}
