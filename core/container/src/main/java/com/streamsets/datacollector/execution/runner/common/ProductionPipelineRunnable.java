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

import com.streamsets.datacollector.el.JobEL;
import com.streamsets.datacollector.el.PipelineEL;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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
  private volatile boolean isStopped = false;
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
      PipelineInfo info = pipeline.getPipelineConf().getInfo();
      if(info != null) {
        Thread.currentThread().setName(Utils.format("{}-{}-{}", RUNNABLE_NAME, info.getPipelineId(), info.getTitle()));
      } else {
        Thread.currentThread().setName(Utils.format("{}-UNKNOWN_ID-{}", RUNNABLE_NAME, name));
      }
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
        cancelTask();
      }
    } finally {
      PipelineEL.unsetConstantsInContext();
      JobEL.unsetConstantsInContext();
      postStop();
      countDownLatch.countDown();
      Thread.currentThread().setName(originalThreadName);
    }
  }

  public void stop(boolean nodeProcessShutdown) throws PipelineException {
    this.isStopped = true;
    this.nodeProcessShutdown = nodeProcessShutdown;
    pipeline.stop();
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted: {}", e.toString(), e);
    }

    // Comes here when thread is interrupted and run() did post-stop process, or
    // when pipeline is stopped by force quit.
    LOG.info("Pipeline is in terminal state");
  }

  /**
   * This function is called while thread is waiting at countDownLatch.await in stop().
   * It cancels tasks, change pipeline state and call countDown(), so that the waiting thread
   * can proceed to terminate.
   */
  public void forceQuit() {
    synchronized (relatedTasks){
      if (runningThread != null) {
        runningThread.interrupt();
        runningThread = null;
        cancelTask();
        postStop();
      }
    }
    countDownLatch.countDown();
  }

  private void cancelTask(){
    //signal observer thread [which shares this object] to stop
    for (Future<?> task : relatedTasks) {
      LOG.debug("Cancelling task " + task);
      task.cancel(true);
    }
  }

  private void postStop() {
    Thread.currentThread().setName(Thread.currentThread().getName());
    //Update pipeline state accordingly
    if (pipeline.wasStopped() && !pipeline.isExecutionFailed()) {
      try {
        Map<String, String> offset = pipeline.getCommittedOffsets();
        String offsetStatus = "";

        if (!offset.isEmpty()) {
          if(offset.size() == 1 && offset.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
            offsetStatus = Utils.format("The last committed source offset is '{}'.", offset.get(Source.POLL_SOURCE_OFFSET_KEY));
          } else {
            offsetStatus = Utils.format("The last committed source offset is {}.", offset);
          }
        }

        if (this.nodeProcessShutdown) {
          LOG.info("Changing state of pipeline '{}', '{}' to '{}'", name, rev, PipelineStatus.DISCONNECTED);
          pipeline.getStatusListener().stateChanged(
              PipelineStatus.DISCONNECTED,
              "The pipeline was stopped because the node process was shutdown. " + offsetStatus,
              null
          );
        } else {
          LOG.info("Changing state of pipeline '{}', '{}' to '{}'", name, rev, PipelineStatus.STOPPED);
          pipeline.getStatusListener().stateChanged(
              PipelineStatus.STOPPED,
              "The pipeline was stopped. " + offsetStatus,
              null
          );
        }
      } catch (PipelineException e) {
        LOG.error("An exception occurred while trying to transition pipeline state, {}", e.toString(), e);
      }
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
