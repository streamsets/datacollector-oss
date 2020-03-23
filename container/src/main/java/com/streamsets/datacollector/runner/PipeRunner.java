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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.lib.log.LogConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pipe Runner that wraps one source-less instance of the pipeline.
 */
public class PipeRunner {

  private static final Logger LOG = LoggerFactory.getLogger(PipeRunner.class);

  public static final String METRIC_BATCH_COUNT = "batchCount";
  public static final String METRIC_OFFSET_KEY = "offsetKey";
  public static final String METRIC_OFFSET_VALUE = "offsetValue";
  public static final String METRIC_CURRENT_STAGE = "currentStage";
  public static final String METRIC_BATCH_START_TIME = "batchStartTime";
  public static final String METRIC_STAGE_START_TIME = "stageStartTime";
  public static final String METRIC_STATE = "state";

  public static final String IDLE = "IDLE";

  @FunctionalInterface
  public interface ThrowingConsumer<T> {
    public void accept(T t) throws PipelineRuntimeException, StageException;
  }

  /**
   * Runner id.
   *
   * Number from 0 to N denoting "instance number" of this runner.
   */
  private final int runnerId;

  /**
   * Pipe instances for this runner.
   */
  private final List<Pipe> pipes;

  /**
   * Gauge with runtime metrics of this runner.
   */
  private final Map<String, Object> runtimeMetricGauge;

  public PipeRunner(
      String pipelineName,
      String pipelineRev,
      int runnerId,
      MetricRegistry metricRegistry,
      List<Pipe> pipes
  ) {
    this.runnerId = runnerId;
    this.pipes = ImmutableList.copyOf(pipes);

    // Create metric gauge for this particular runner
    this.runtimeMetricGauge = MetricsConfigurator.createStageGauge(
      metricRegistry,
      "runner." + runnerId,
      null,
      pipelineName,
      pipelineRev
    ).getValue();

    // And fill in default values
    this.runtimeMetricGauge.put(METRIC_BATCH_COUNT, 0L);
    resetBatchSpecificMetrics();
  }

  public Pipe get(int i) {
    return pipes.get(i);
  }

  public int getRunnerId() {
    return runnerId;
  }

  public int size() {
    return pipes.size();
  }

  public List<Pipe> getPipes() {
    return pipes;
  }

  /**
   * Run batch with given consumer for each pipe.
   *
   * This method will also set the logger appropriately and calculate the runner specific metrics.
   */
  public void executeBatch(
      String offsetKey,
      String offsetValue,
      long batchStartTime,
      ThrowingConsumer<Pipe> consumer
  ) throws PipelineRuntimeException, StageException {
    this.runtimeMetricGauge.put(METRIC_STATE, "Processing Batch");
    this.runtimeMetricGauge.put(METRIC_BATCH_START_TIME, batchStartTime);
    this.runtimeMetricGauge.put(METRIC_OFFSET_KEY, Optional.ofNullable(offsetKey).orElse(""));
    this.runtimeMetricGauge.put(METRIC_OFFSET_VALUE, Optional.ofNullable(offsetValue).orElse(""));

    forEachInternal(consumer);

    // We've successfully finished batch
    this.runtimeMetricGauge.computeIfPresent(METRIC_BATCH_COUNT, (key, value) -> ((long)value) + 1);
  }

  private void forEachInternal(ThrowingConsumer<Pipe> consumer) throws PipelineRuntimeException {
    MDC.put(LogConstants.RUNNER, String.valueOf(runnerId));

    try {
      // Run one pipe at a time
      for(Pipe p : pipes) {
        String instanceName = p.getStage().getInfo().getInstanceName();
        this.runtimeMetricGauge.put(METRIC_STAGE_START_TIME, System.currentTimeMillis());
        this.runtimeMetricGauge.put(METRIC_CURRENT_STAGE, instanceName);
        MDC.put(LogConstants.STAGE, instanceName);
        if (p instanceof StagePipe) {
          this.runtimeMetricGauge.put(METRIC_STAGE_START_TIME, System.currentTimeMillis());
        }

        acceptConsumer(consumer, p);
      }
    } finally {
      resetBatchSpecificMetrics();
      MDC.put(LogConstants.RUNNER, "");
      MDC.put(LogConstants.STAGE, "");
    }
  }

  private void resetBatchSpecificMetrics() {
    // Fill in default values when there is no batch running
    this.runtimeMetricGauge.put(METRIC_CURRENT_STAGE, IDLE);
    this.runtimeMetricGauge.put(METRIC_STATE, "");
    this.runtimeMetricGauge.put(METRIC_OFFSET_KEY, "");
    this.runtimeMetricGauge.put(METRIC_OFFSET_VALUE, "");
    this.runtimeMetricGauge.put(METRIC_BATCH_START_TIME, 0L);
  }

  /**
   * Execute given consumer for each pipe, rethrowing usual exceptions as RuntimeException.
   *
   * Suitable for consumer that is not suppose to throw PipelineException and StageException. This method will
   * not calculate usual stage metrics.
   */
  public void forEach(String reportedState, ThrowingConsumer<Pipe> consumer) {
    this.runtimeMetricGauge.put(METRIC_STATE, reportedState);
    this.runtimeMetricGauge.put(METRIC_BATCH_START_TIME, System.currentTimeMillis());
    try {
      forEachInternal(consumer);
    } catch (PipelineException|StageException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve OffsetCommitTrigger pipe.
   *
   * If it exists, null otherwise.
   */
  public OffsetCommitTrigger getOffsetCommitTrigger() {
    for (Pipe pipe : pipes) {
      Stage stage = pipe.getStage().getStage();
      if (stage instanceof Target && stage instanceof OffsetCommitTrigger) {
        return (OffsetCommitTrigger) stage;
      }
    }
    return null;
  }

  /**
   * Accept given consumer and proper log context of any exception.
   */
  private void acceptConsumer(ThrowingConsumer<Pipe> consumer, Pipe p) throws PipelineRuntimeException, StageException {
    try {
      // Process pipe
      consumer.accept(p);
    } catch (Throwable t) {
      String instanceName = p.getStage().getInfo().getInstanceName();
      LOG.error("Failed executing stage '{}': {}", instanceName, t.toString(), t);
      Throwables.propagateIfInstanceOf(t, PipelineRuntimeException.class);
      Throwables.propagateIfInstanceOf(t, StageException.class);
      Throwables.propagate(t);
    }
  }
}
