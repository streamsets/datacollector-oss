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
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.IssuesJson;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
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
    boolean errorWhileRunning = false;
    boolean isRecoverable = true;
    String runningErrorMsg = "";
    try {
      try {
        LOG.debug("Initializing");
        List<Issue> issues = null;
        try {
          issues = getPipeline().init();
        } catch (Throwable e) {
          if (!wasStopped()) {
            LOG.warn("Error while starting: {}", e.toString(), e);
            stateChanged(PipelineStatus.START_ERROR, e.toString(), null);
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
              stateChanged(PipelineStatus.RUNNING_ERROR, runningErrorMsg, null);
              errorWhileRunning = true;
              isRecoverable = isRecoverableThrowable(e);
            }
            throw e;
          }
        } else {
          LOG.debug("Stopped due to validation error");
          PipelineRuntimeException e = new PipelineRuntimeException(ContainerError.CONTAINER_0800, name,
            issues.get(0).getMessage());
          Map<String, Object> attributes = new HashMap<>();
          attributes.put("issues", new IssuesJson(new Issues(issues)));
          stateChanged(PipelineStatus.START_ERROR, issues.get(0).getMessage(), attributes);
          getPipeline().errorNotification(e);
          throw e;
        }
      } finally {
        LOG.debug("Destroying");

        try {
          pipeline.destroy();
        } catch (Throwable e) {
          LOG.warn("Error while calling destroy: " + e, e);
          throw e;
        } finally {
          // if the destroy throws an Exception but pipeline.run() finishes well,
          // me move to finished state
          if (finishing) {
            LOG.debug("Finished");
            stateChanged(PipelineStatus.FINISHED, null, null);
          } else if (errorWhileRunning) {
            LOG.debug("Stopped due to an error");
            if (shouldRetry && !pipeline.shouldStopOnStageError() && !isExecutingInSlave && isRecoverable) {
              stateChanged(PipelineStatus.RETRY, runningErrorMsg, null);
            } else {
              stateChanged(PipelineStatus.RUN_ERROR, runningErrorMsg, null);
            }
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

  public String getCommittedOffset() {
    return pipelineRunner.getCommittedOffset();
  }

  public void captureSnapshot(String snapshotName, int batchSize, int batches) {
    pipelineRunner.capture(snapshotName, batchSize, batches);
  }

  public void cancelSnapshot(String snapshotName) throws PipelineException {
    pipelineRunner.cancelSnapshot(snapshotName);
  }

  public void setOffset(String offset) {
    ProductionSourceOffsetTracker offsetTracker = (ProductionSourceOffsetTracker) pipelineRunner.getOffSetTracker();
    offsetTracker.setOffset(offset);
    offsetTracker.commitOffset();
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
