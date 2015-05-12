/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.StandalonePipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

public class ProductionPipelineRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunnable.class);
  public static final String RUNNABLE_NAME = "ProductionPipelineRunnable";

  private final StandalonePipelineManagerTask pipelineManager;
  private final ProductionPipeline pipeline;
  private final String name;
  private final String rev;
  private volatile Thread runningThread;
  private volatile boolean nodeProcessShutdown;
  private boolean exception;
  private final List<Future<?>> relatedTasks;


  public ProductionPipelineRunnable(ThreadHealthReporter threadHealthReporter,
                                    StandalonePipelineManagerTask pipelineManager, ProductionPipeline pipeline,
                                    String name, String rev, List<Future<?>> relatedTasks) {
    this.pipelineManager = pipelineManager;
    this.pipeline = pipeline;
    this.rev = rev;
    this.name = name;
    this.relatedTasks = relatedTasks;
    this.pipeline.setThreadHealthReporter(threadHealthReporter);
  }

  @Override
  public void run() {
    String originalThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(originalThreadName + "-" + RUNNABLE_NAME);
      try {
        runningThread = Thread.currentThread();
        pipeline.run();
      } catch (Exception e) {
        if(!pipeline.wasStopped()) {
          LOG.error("An exception occurred while running the pipeline, {}", e.getMessage(), e);
          try {
            pipelineManager.setState(name, rev, State.ERROR, e.getMessage(), pipelineManager.getMetrics());
          } catch (PipelineManagerException ex) {
            LOG.error("An exception occurred while committing the state, {}", ex.getMessage(), e);
          }
          exception = true;
        }
      } catch (Error e) {
        LOG.error("A JVM error occurred while running the pipeline, {}", e.getMessage(), e);
        try {
          pipelineManager.setState(name, rev, State.ERROR, e.getMessage(), pipelineManager.getMetrics());
        } catch (PipelineManagerException ex) {
          LOG.error("An exception occurred while committing the state, {}", ex.getMessage(), e);
        }
        throw e;
      } finally {
        runningThread = null;
        //signal observer thread [which shares this object] to stop
        for (Future<?> task : relatedTasks) {
          task.cancel(true);
        }
      }

      //Update pipeline state accordingly
      if (pipeline.wasStopped()) {
        //pipeline was stopped while it was running, could be pipeline stop or node process shutdown [Ctrl-C]
        try {
          if (this.nodeProcessShutdown) {
            pipelineManager.validateStateTransition(name, rev, State.NODE_PROCESS_SHUTDOWN);
            pipelineManager.setState(name, rev, State.NODE_PROCESS_SHUTDOWN,
              Utils.format("The pipeline was stopped because the node process was shutdown. " +
                "The last committed source offset is {}.", pipeline.getCommittedOffset()), pipelineManager.getMetrics());
          } else {
            pipelineManager.validateStateTransition(name, rev, State.STOPPED);
            pipelineManager.setState(name, rev, State.STOPPED,
              Utils.format("The pipeline was stopped. The last committed source offset is {}."
                , pipeline.getCommittedOffset()), pipelineManager.getMetrics());
          }
        } catch (PipelineManagerException e) {
          LOG.error("An exception occurred while stopping the pipeline, {}", e.getMessage(), e);
        }
      } else if (exception) {
        //Pipeline stopped because of exception
        //State is already updated to ERROR, No-op
      } else {
        //pipeline execution finished normally
        try {
          pipelineManager.validateStateTransition(name, rev, State.FINISHED);
          pipelineManager.setState(name, rev, State.FINISHED, "Completed successfully.", pipelineManager.getMetrics());
        } catch (PipelineManagerException e) {
          LOG.error("An exception occurred while finishing the pipeline, {}", e.getMessage(), e);
        }
      }
    } finally {
      Thread.currentThread().setName(originalThreadName);
    }
  }

  public void stop(boolean nodeProcessShutdown) {
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
}
