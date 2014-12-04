/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductionPipelineRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunnable.class);

  private final ProductionPipelineManagerTask pipelineManager;
  private final ProductionPipeline pipeline;
  private final String name;
  private final String rev;

  public ProductionPipelineRunnable(ProductionPipelineManagerTask pipelineManager, ProductionPipeline pipeline,
                                    String name, String rev) {
    this.pipelineManager = pipelineManager;
    this.pipeline = pipeline;
    this.rev = rev;
    this.name = name;
  }

  @Override
  public void run() {
    try {
      pipeline.run();
    } catch (Exception e) {
      LOG.error(Utils.format("An exception occurred while running the pipeline, {}", e.getMessage()));
      try {
        pipelineManager.setState(name, rev, State.ERROR, e.getMessage());
      } catch (PipelineManagerException ex) {
        LOG.error(Utils.format("An exception occurred while committing the state, {}", ex.getMessage()));
      }
    } catch (Error e) {
      LOG.error(Utils.format("A JVM error occurred while running the pipeline, {}", e.getMessage()));
      throw e;
    }

    if(pipeline.wasStopped()) {
      //pipeline was stopped while it was running
      try {
        pipelineManager.validateStateTransition(State.STOPPED);
        pipelineManager.setState(name, rev, State.STOPPED,
          Utils.format("The pipeline was stopped. The last committed source offset is {}."
              , pipeline.getCommittedOffset()));
      } catch (PipelineManagerException e) {
        LOG.error(Utils.format("An exception occurred while stopping the pipeline, {}", e.getMessage()));
      }
    } else {
      //pipeline execution finished normally
      try {
        pipelineManager.validateStateTransition(State.FINISHED);
        pipelineManager.setState(name, rev, State.FINISHED, "Completed successfully.");
      } catch (PipelineManagerException e) {
        LOG.error(Utils.format("An exception occurred while finishing the pipeline, {}", e.getMessage()));
      }
    }
  }

  public void stop() {
    pipeline.stop();
  }

  public String getRev() {
    return rev;
  }

  public String getName() {
    return name;
  }
}
