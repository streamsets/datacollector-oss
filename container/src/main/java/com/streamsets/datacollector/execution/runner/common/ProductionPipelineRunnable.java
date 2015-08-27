/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
  private final CountDownLatch countDownLatch;

  public ProductionPipelineRunnable(ThreadHealthReporter threadHealthReporter,
                                    StandaloneRunner runner, ProductionPipeline pipeline,
                                    String name, String rev, List<Future<?>> relatedTasks) {
    this.runner = runner;
    this.pipeline = pipeline;
    this.rev = rev;
    this.name = name;
    this.relatedTasks = relatedTasks;
    this.pipeline.setThreadHealthReporter(threadHealthReporter);
    this.countDownLatch = new CountDownLatch(1);
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
          LOG.error("An exception occurred while running the pipeline, {}", e.toString(), e);
        }
      } catch (Error e) {
        LOG.error("A JVM error occurred while running the pipeline, {}", e.toString(), e);
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
          LOG.error("An exception occurred while trying to transition pipeline state, {}", e.toString(), e);
        }
      }
    } finally {
      Thread.currentThread().setName(originalThreadName);
      countDownLatch.countDown();
    }

  }

  public void stop(boolean nodeProcessShutdown) throws PipelineException {
    this.isStopped = true;
    this.nodeProcessShutdown = nodeProcessShutdown;
    pipeline.stop();
    Thread thread = runningThread;
    if (thread != null) {
      // cannot interrupt the thread as it does not play well with writing to HDFS
      // this causes issues in batch mode when we are trying to rename files on stop
      thread.interrupt();
      LOG.info("Pipeline stopped, thread '{}' running the pipeline", thread.getName());
    }
    boolean isDone = false;
    try {
      isDone = countDownLatch.await(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted: {}", e.toString(), e);
    }
    if (!isDone) {
      LOG.warn("Pipeline is not done yet");
    } else {
      LOG.info("Pipeline is in terminal state");
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
