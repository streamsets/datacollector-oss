/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner.common;

import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ThreadHealthReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

public class ProductionPipelineRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunnable.class);
  public static final String RUNNABLE_NAME = "ProductionPipelineRunnable";
  private final StandaloneRunner runner;
  private final ProductionPipeline pipeline;
  private final String name;
  private final String rev;
  private volatile Thread runningThread;
  private volatile boolean nodeProcessShutdown;
  private final List<Future<?>> relatedTasks;
  private volatile boolean isStopped;

  public ProductionPipelineRunnable(ThreadHealthReporter threadHealthReporter,
                                    StandaloneRunner runner, ProductionPipeline pipeline,
                                    String name, String rev, List<Future<?>> relatedTasks) {
    this.runner = runner;
    this.pipeline = pipeline;
    this.rev = rev;
    this.name = name;
    this.relatedTasks = relatedTasks;
    this.pipeline.setThreadHealthReporter(threadHealthReporter);
  }

  @Override
  public void run() {
    if (isStopped) {
      throw new IllegalStateException(Utils.format("Pipeline is stopped, cannot start the pipeline '{}::{}'", name, rev));
    }
    String originalThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(originalThreadName + "-" + RUNNABLE_NAME);
      try {
        runningThread = Thread.currentThread();
        pipeline.run();
      } catch (Exception e) {
        if(!pipeline.wasStopped()) {
          LOG.error("An exception occurred while running the pipeline, {}", e.getMessage(), e);
        }
      } catch (Error e) {
        LOG.error("A JVM error occurred while running the pipeline, {}", e.getMessage(), e);
        // may be go to run_error
        throw e;
      } finally {
        // set state to error
        runningThread = null;
        //signal observer thread [which shares this object] to stop
        for (Future<?> task : relatedTasks) {
          LOG.info("Cancelling task " + task);
          task.cancel(true);
        }
      }

      //Update pipeline state accordingly
      if (pipeline.wasStopped()) {
        try {
          if (this.nodeProcessShutdown) {
            LOG.info("Changing state of pipeline '{}', '{}' to '{}'", name, rev, PipelineStatus.DISCONNECTED);
              pipeline.getStatusListener().stateChanged(PipelineStatus.DISCONNECTED, Utils.format("The pipeline was stopped "
                + "because the node process was shutdown. " +
                "The last committed source offset is {}.", pipeline.getCommittedOffset(), runner.getMetrics()), null);
          } else {
            LOG.info("Changing state of pipeline '{}', '{}' to '{}'", name, rev, PipelineStatus.STOPPED);
            pipeline.getStatusListener().stateChanged(PipelineStatus.STOPPED,
              Utils.format("The pipeline was stopped. The last committed source offset is {}."
                , pipeline.getCommittedOffset()), null);
          }
        } catch (PipelineRuntimeException e) {
          LOG.error("An exception occurred while trying to transition pipeline state, {}", e.getMessage(), e);
        }
      }
    } finally {
      Thread.currentThread().setName(originalThreadName);
    }
  }

  public void stop(boolean nodeProcessShutdown) {
    this.isStopped = true;
    this.nodeProcessShutdown = nodeProcessShutdown;
    pipeline.stop();
    Thread thread = runningThread;
    if (thread != null) {
      thread.interrupt();
      LOG.debug("Pipeline stopped, interrupting the thread running the pipeline");
    }
  }

  public String getRev() {
    return rev;
  }

  public String getName() {
    return name;
  }

  public boolean isStopped() {
    return isStopped;
  }

}
