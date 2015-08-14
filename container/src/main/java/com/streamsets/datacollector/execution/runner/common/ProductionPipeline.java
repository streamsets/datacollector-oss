/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.IssuesJson;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.Issues;
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

  private final RuntimeInfo runtimeInfo;
  private final PipelineConfiguration pipelineConf;
  private final Pipeline pipeline;
  private final ProductionPipelineRunner pipelineRunner;
  private StateListener stateListener;
  private volatile PipelineStatus pipelineStatus;
  private final String name;
  private final String rev;

  public ProductionPipeline(String name, String rev, RuntimeInfo runtimeInfo, PipelineConfiguration pipelineConf,
                            Pipeline pipeline) {
    this.name = name;
    this.rev = rev;
    this.runtimeInfo = runtimeInfo;
    this.pipelineConf = pipelineConf;
    this.pipeline = pipeline;
    this.pipelineRunner =  (ProductionPipelineRunner)pipeline.getRunner();
  }

  public StateListener getStatusListener() {
    return this.stateListener;
  }

  public void registerStatusListener(StateListener stateListener) {
    this.stateListener = stateListener;
  }

  private void stateChanged(PipelineStatus pipelineStatus, String message, Map<String, Object> attributes)
    throws PipelineRuntimeException {
    this.pipelineStatus = pipelineStatus;
    if (stateListener != null) {
      stateListener.stateChanged(pipelineStatus, message, attributes);
    }
  }

  public void run() throws StageException, PipelineRuntimeException {
    boolean finishing = false;
    boolean errorWhileRunning = false;
    String runningErrorMsg = "";
    try {
      try {
        LOG.debug("Initializing");
        List<Issue> issues = null;
        try {
          issues = pipeline.init();
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
            }
            throw e;
          }
        } else {
          LOG.debug("Stopped due to validation error");
          PipelineRuntimeException e = new PipelineRuntimeException(ContainerError.CONTAINER_0800,
            issues.get(0).getMessage());
          Map<String, Object> attributes = new HashMap<>();
          attributes.put("issues", new IssuesJson(new Issues(issues)));
          stateChanged(PipelineStatus.START_ERROR, issues.get(0).getMessage(), attributes);
          throw e;
        }
      } finally {
        LOG.debug("Destroying");
        pipeline.destroy();
        if (pipeline.getSource() instanceof ClusterSource) {
          LOG.debug("Calling cluster source post destroy");
          ((ClusterSource) pipeline.getSource()).postDestroy();
        }
        if (finishing) {
          LOG.debug("Finished");
          stateChanged(PipelineStatus.FINISHED, null, null);
        } else if (errorWhileRunning) {
          LOG.debug("Stopped due to an error");
          stateChanged(PipelineStatus.RUN_ERROR, runningErrorMsg, null);
        }
      }
    } finally {
      MetricsConfigurator.cleanUpJmxMetrics(name, rev);
    }
  }

  public PipelineConfiguration getPipelineConf() {
    return pipelineConf;
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  public void stop() throws PipelineException {
    pipelineRunner.stop();
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
