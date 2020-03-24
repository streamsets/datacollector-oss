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
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.IssuesJson;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProductionPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipeline.class);
  private final PipelineConfiguration pipelineConf;
  private final Pipeline pipeline;
  private final ProductionPipelineRunner pipelineRunner;
  private StateListener stateListener;
  private final String name;
  private final String rev;
  private final boolean isExecutingInSlave;
  private final boolean shouldRetry;
  private boolean executionFailed;

  public ProductionPipeline(String name, String rev, PipelineConfiguration pipelineConf,
                            Configuration conf, Pipeline pipeline, boolean shouldRetry) {
    this.name = name;
    this.rev = rev;
    this.pipelineConf = pipelineConf;
    this.pipeline = pipeline;
    this.pipelineRunner =  (ProductionPipelineRunner)pipeline.getRunner();
    ExecutionMode executionMode =
      ExecutionMode.valueOf(conf.get(RuntimeModule.PIPELINE_EXECUTION_MODE_KEY, ExecutionMode.STANDALONE.name()));
    isExecutingInSlave = (executionMode == ExecutionMode.SLAVE);
    this.shouldRetry = shouldRetry;
  }

  public StateListener getStatusListener() {
    return this.stateListener;
  }

  public void registerStatusListener(StateListener stateListener) {
    this.stateListener = stateListener;
  }

  private void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
    throws PipelineRuntimeException {
    stateListener.stateChanged(pipelineStatus, message, attributes);
  }

  public void run() throws StageException, PipelineRuntimeException {
    boolean finishing = false;
    boolean errorWhileInitializing = false;
    boolean errorWhileRunning = false;
    Throwable runningException = null;
    boolean errorWhileDestroying = false;
    boolean isRecoverable = true;
    executionFailed = false;
    String runningErrorMsg = null;
    try {
      try {
        LOG.debug("Initializing");
        List<Issue> issues = null;
        try {
          issues = getPipeline().init(true);
        } catch (Throwable e) {
          if (!wasStopped()) {
            runningErrorMsg = e.toString();
            LOG.warn("Error while starting: {}", e.toString(), e);
            errorWhileInitializing = true;
            stateChanged(PipelineStatus.STARTING_ERROR, e.toString(), null);
          }
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0702, e.toString(), e);
        }
        if (issues.isEmpty()) {
          try {
            stateChanged(PipelineStatus.RUNNING, null, null);
            LOG.debug("Running");
            pipeline.run();
            if (!wasStopped()) {
              LOG.debug("Finishing");
              stateChanged(PipelineStatus.FINISHING, null, null);
              finishing = true;
            }
          } catch (Throwable e) {
            if (!wasStopped()) {
              runningErrorMsg = e.toString();
              LOG.warn("Error while running: {}", runningErrorMsg, e);

              // Make sure that the whole error is serialized in the status file
              Map<String, Object> extraAttributes = new HashMap<>();
              if(e instanceof StageException) {
                extraAttributes.put(AbstractRunner.ANTENNA_DOCTOR_MESSAGES_ATTR, ((StageException) e).getAntennaDoctorMessages());
                runningErrorMsg = e.getMessage();
              }
              extraAttributes.put(AbstractRunner.ERROR_MESSAGE_ATTR, e.getMessage());
              extraAttributes.put(AbstractRunner.ERROR_STACKTRACE_ATTR, ErrorMessage.toStackTrace(e));

              stateChanged(PipelineStatus.RUNNING_ERROR, runningErrorMsg, extraAttributes);
              errorWhileRunning = true;
              runningException = e;
              isRecoverable = isRecoverableThrowable(e);
            }
            throw e;
          }
        } else {
          LOG.info("Stopped due to validation error");
          issues.forEach(i -> LOG.info("\tValidation issue: {}", i.getMessage()));
          PipelineRuntimeException e = new PipelineRuntimeException(ContainerError.CONTAINER_0800, issues.size(), issues.get(0).getMessage());
          Map<String, Object> attributes = new HashMap<>();
          attributes.put("issues", new IssuesJson(new Issues(issues)));
          attributes.put(AbstractRunner.ANTENNA_DOCTOR_MESSAGES_ATTR, issues.get(0).getAntennaDoctorMessages());
          attributes.put(AbstractRunner.ERROR_MESSAGE_ATTR, e.getMessage());
          attributes.put(AbstractRunner.ERROR_STACKTRACE_ATTR, ErrorMessage.toStackTrace(e));
          // We need to store the error in runningErrorMsg, so that it gets propagated to START_ERROR terminal state
          runningErrorMsg = issues.get(0).getMessage();
          stateChanged(PipelineStatus.STARTING_ERROR, runningErrorMsg, attributes);
          errorWhileInitializing = true;
          getPipeline().errorNotification(e);
          throw e;
        }
      } finally {
        LOG.debug("Destroying");
        try {
          // Determine the reason why we got all the way here
          PipelineStopReason stopReason;
          if(errorWhileRunning || errorWhileInitializing) {
            stopReason = PipelineStopReason.FAILURE;
          } else if(wasStopped()) {
            stopReason = PipelineStopReason.USER_ACTION;
          } else {
            stopReason = PipelineStopReason.FINISHED;
          }
          // Destroy the pipeline
          pipeline.destroy(true, stopReason);
        } catch (Throwable e) {
          LOG.warn("Error while calling destroy: " + e.toString(), e);
          stateChanged(PipelineStatus.STOPPING_ERROR, e.toString(), null);
          errorWhileDestroying = true;
          // If this is the first error that happened during the execution, persist the reasoning in the message, otherwise
          // keep the original message so that terminal state have the original error rather then any subsequent one.
          if(runningErrorMsg == null) {
            runningErrorMsg = e.toString();
          }
          throw e;
        } finally {
          if(errorWhileInitializing || errorWhileRunning || errorWhileDestroying) {
              boolean retry = true;
            // Check if the exception is an onRecordErrorException
            // The DefaultErrorRecordHandler throws this exception back when the value is set to STOP PIPELINE.
            // In such case, do not retry the pipeline. Let it transition to a error state and stop.
            if (errorWhileRunning && runningException instanceof OnRecordErrorException) {
              retry = false;
            }

            // In case of any error, persist that information
            executionFailed = true;

            // If there was any problem, we will consider retry
            if (shouldRetry && retry && !isExecutingInSlave && isRecoverable && !wasStopped()) {
              stateChanged(PipelineStatus.RETRY, runningErrorMsg, null);
            } else if(errorWhileInitializing) {
              stateChanged(PipelineStatus.START_ERROR, runningErrorMsg, null);
            } else if(errorWhileRunning) {
              stateChanged(PipelineStatus.RUN_ERROR, runningErrorMsg, null);
            } else if(errorWhileDestroying) {
              stateChanged(PipelineStatus.STOP_ERROR, runningErrorMsg, null);
            }
          } else if(finishing) {
            // Graceful shutdown
            LOG.debug("Finished");
            stateChanged(PipelineStatus.FINISHED, null, null);
          }

          if (isExecutingInSlave) {
            LOG.debug("Calling cluster source post destroy");
            ((ClusterSource) pipeline.getSource()).postDestroy();
          }
        }
      }
    } finally {
      MetricsConfigurator.cleanUpJmxMetrics(name, rev);
    }
  }

  /**
   * Does it make sense to re-run the pipeline (if allowed) after
   * given Throwable was thrown while running the pipeline.
   */
  private boolean isRecoverableThrowable(Throwable e) {
    if(e instanceof Error) {
      return false;
    }

    return true;
  }

  public PipelineConfiguration getPipelineConf() {
    return pipelineConf;
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  public void stop() throws PipelineException {
    pipelineRunner.stop();
    pipeline.stop();
  }

  public boolean wasStopped() {
    return pipelineRunner.wasStopped();
  }

  public boolean isExecutionFailed() {
    return executionFailed;
  }

  /**
   * Returns committed offsets.
   *
   * This method returns unmodifiable view of the map where we're maintaining offsets - hence this is not
   * a snapshot and as additional offsets are committed this map and the return view will change.
   */
  public Map<String, String> getCommittedOffsets() {
    return pipelineRunner.getCommittedOffsets();
  }

  public void captureSnapshot(String snapshotName, int batchSize, int batches) {
    pipelineRunner.capture(snapshotName, batchSize, batches);
  }

  public void cancelSnapshot(String snapshotName) throws PipelineException {
    pipelineRunner.cancelSnapshot(snapshotName);
  }

  public List<Record> getErrorRecords(String instanceName, int size) {
    return pipelineRunner.getErrorRecords(instanceName, size);
  }

  public List<ErrorMessage> getErrorMessages(String instanceName, int size) {
    return pipelineRunner.getErrorMessages(instanceName, size);
  }

  public long getLastBatchTime() {
    return pipelineRunner.getOffSetTracker().getLastBatchTime();
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    pipelineRunner.setThreadHealthReporter(threadHealthReporter);
  }
}
