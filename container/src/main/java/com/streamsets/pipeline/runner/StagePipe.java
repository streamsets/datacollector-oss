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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.memory.MemoryMonitor;
import com.streamsets.pipeline.memory.MemoryUsageCollector;
import com.streamsets.pipeline.memory.MemoryUsageCollectorResourceBundle;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.streamsets.pipeline.validation.Issue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StagePipe extends Pipe<StagePipe.Context> {

  private static final Logger LOG = LoggerFactory.getLogger(StagePipe.class);
  //Runtime stat gauge name
  public static final String RUNTIME_STATS_GAUGE = "RuntimeStatsGauge";
  private Timer processingTimer;
  private Counter inputRecordsCounter;
  private Counter outputRecordsCounter;
  private Counter errorRecordsCounter;
  private Counter stageErrorCounter;
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

  @VisibleForTesting
  StagePipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    this("myPipeline", "0", stage, inputLanes, outputLanes, new ResourceControlledScheduledExecutor(0.02f),
      new MemoryUsageCollectorResourceBundle());
  }

  public StagePipe(String name, String rev, StageRuntime stage, List<String> inputLanes, List<String> outputLanes,
                   ResourceControlledScheduledExecutor scheduledExecutorService,
                   MemoryUsageCollectorResourceBundle memoryUsageCollectorResourceBundle) {
    super(stage, inputLanes, outputLanes);
    this.name = name;
    this.rev = rev;
    this.scheduledExecutorService = scheduledExecutorService;
    this.memoryUsageCollectorResourceBundle = memoryUsageCollectorResourceBundle;
  }

  @Override
  public List<Issue> init(StagePipe.Context pipeContext) throws StageException {
    List<Issue> issues = getStage().init();
    if(issues.isEmpty()) {
      MetricRegistry metrics = getStage().getContext().getMetrics();
      String metricsKey = "stage." + getStage().getConfiguration().getInstanceName();
      processingTimer = MetricsConfigurator.createTimer(metrics, metricsKey + ".batchProcessing", name, rev);
      inputRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".inputRecords", name, rev);
      outputRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".outputRecords", name, rev);
      errorRecordsCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".errorRecords", name, rev);
      stageErrorCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".stageErrors", name, rev);
      memoryConsumedCounter = MetricsConfigurator.createCounter(metrics, metricsKey + ".memoryConsumed", name, rev);
      inputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".inputRecords", name, rev);
      outputRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".outputRecords", name, rev);
      errorRecordsMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".errorRecords", name, rev);
      stageErrorMeter = MetricsConfigurator.createMeter(metrics, metricsKey + ".stageErrors", name, rev);
      inputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".inputRecords", name, rev);
      outputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".outputRecords", name, rev);
      errorRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".errorRecords", name, rev);
      stageErrorsHistogram = MetricsConfigurator.createHistogram5Min(metrics, metricsKey + ".stageErrors", name, rev);
      if (getStage().getConfiguration().getOutputLanes().size() > 1) {
        outputRecordsPerLaneCounter = new HashMap<>();
        outputRecordsPerLaneMeter = new HashMap<>();
        for (String lane : getStage().getConfiguration().getOutputLanes()) {
          outputRecordsPerLaneCounter.put(lane, MetricsConfigurator.createCounter(
              metrics, metricsKey + ":" + lane + ".outputRecords", name, rev));
          outputRecordsPerLaneMeter.put(lane, MetricsConfigurator.createMeter(
              metrics, metricsKey + ":" + lane + ".outputRecords", name, rev));
        }
      }
      this.context = pipeContext;
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

  public long getMemoryConsumed() {
    return memoryConsumedCounter.getCount();
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
