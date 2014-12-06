/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.metrics.MetricsConfigurator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StagePipe extends Pipe {
  private Timer processingTimer;
  private Counter inputRecordsCounter;
  private Counter outputRecordsCounter;
  private Counter errorRecordsCounter;
  private Counter stageErrorCounter;
  private Meter inputRecordsMeter;
  private Meter outputRecordsMeter;
  private Meter errorRecordsMeter;
  private Meter stageErrorMeter;
  private Map<String, Counter> outputRecordsPerLaneCounter;
  private Map<String, Meter> outputRecordsPerLaneMeter;

  public StagePipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    super(stage, inputLanes, outputLanes);
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
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    BatchMakerImpl batchMaker = pipeBatch.startStage(this);
    BatchImpl batchImpl = pipeBatch.getBatch(this);
    ErrorSink errorSink = pipeBatch.getErrorSink();
    inputRecordsCounter.inc(batchImpl.getSize());
    inputRecordsMeter.mark(batchImpl.getSize());

    RequiredFieldsErrorPredicateSink predicateSink = new RequiredFieldsErrorPredicateSink(
        getStage().getInfo().getInstanceName(), getStage().getRequiredFields(), errorSink);
    Batch batch = new FilterRecordBatch(batchImpl, predicateSink, predicateSink);

    long start = System.currentTimeMillis();
    String newOffset = getStage().execute(pipeBatch.getPreviousOffset(), pipeBatch.getBatchSize(), batch, batchMaker,
                                          errorSink);
    if (getStage().getDefinition().getType() == StageType.SOURCE) {
      pipeBatch.setNewOffset(newOffset);
    }
    processingTimer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    outputRecordsCounter.inc(batchMaker.getSize());
    outputRecordsMeter.mark(batchMaker.getSize());
    int stageErrorRecordCount = errorSink.getErrorRecords(getStage().getInfo().getInstanceName()).size();
    errorRecordsCounter.inc(predicateSink.size() + stageErrorRecordCount);
    errorRecordsMeter.mark(predicateSink.size() + stageErrorRecordCount);
    int stageErrorsCount = errorSink.getStageErrors(getStage().getInfo().getInstanceName()).size();
    stageErrorCounter.inc(stageErrorsCount);
    stageErrorMeter.mark(stageErrorsCount);
    if (getStage().getConfiguration().getOutputLanes().size() > 1) {
      for (String lane : getStage().getConfiguration().getOutputLanes()) {
        outputRecordsPerLaneCounter.get(lane).inc(batchMaker.getSize(lane));
        outputRecordsPerLaneMeter.get(lane).mark(batchMaker.getSize(lane));
      }
    }
    pipeBatch.completeStage(batchMaker);
  }

  @Override
  public void destroy() {
    getStage().destroy();
  }


}
