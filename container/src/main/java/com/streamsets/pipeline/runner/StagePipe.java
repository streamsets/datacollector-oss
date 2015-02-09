/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.Counter;
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
  public List<StageIssue> validateConfigs() {
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
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    BatchMakerImpl batchMaker = pipeBatch.startStage(this);
    BatchImpl batchImpl = pipeBatch.getBatch(this);
    ErrorSink errorSink = pipeBatch.getErrorSink();

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

    inputRecordsCounter.inc(batchImpl.getSize());
    inputRecordsMeter.mark(batchImpl.getSize());
    inputRecordsHistogram.update(batchImpl.getSize());

    int stageErrorRecordCount = errorSink.getErrorRecords(getStage().getInfo().getInstanceName()).size() +
                                predicateSink.size();
    errorRecordsCounter.inc(stageErrorRecordCount);
    errorRecordsMeter.mark(stageErrorRecordCount);
    errorRecordsHistogram.update(stageErrorRecordCount);

    int outputRecordsCount = batchMaker.getSize();
    if (getStage().getDefinition().getType() == StageType.TARGET) {
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
  }

  @Override
  public void destroy() {
    getStage().destroy();
  }


}
