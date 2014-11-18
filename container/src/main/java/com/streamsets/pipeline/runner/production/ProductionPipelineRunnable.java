/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.prodmanager.PipelineProductionManagerTask;
import com.streamsets.pipeline.prodmanager.PipelineStateException;
import com.streamsets.pipeline.prodmanager.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductionPipelineRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunnable.class);

  private final PipelineProductionManagerTask pipelineManager;
  private final ProductionPipeline pipeline;
  private final String rev;

  public ProductionPipelineRunnable(PipelineProductionManagerTask pipelineManager, ProductionPipeline pipeline,
                                    String rev) {
    this.pipelineManager = pipelineManager;
    this.pipeline = pipeline;
    this.rev = rev;
  }

  @Override
  public void run() {
    try {
      pipeline.run();
      //finished running pipeline without errors.
      //before switching state make sure the transition is valid
      pipelineManager.validateStateTransition(State.NOT_RUNNING);
      if(pipeline.wasStopped()) {
        pipelineManager.setState(rev, State.NOT_RUNNING,
            Utils.format("The pipeline was stopped. The last committed source offset is {}.",
                pipeline.getCommittedOffset()));
      } else {
        pipelineManager.setState(rev, State.NOT_RUNNING, "Completed successfully.");
      }
    } catch (Exception e) {
      LOG.error(Utils.format("An exception occurred while running the pipeline, {}", e.getMessage()));
      try {
        pipelineManager.setState(rev, State.ERROR, e.getMessage());
      } catch (PipelineStateException ex) {
        LOG.error(Utils.format("An exception occurred while committing the state, {}", ex.getMessage()));
      }
    } catch (Error e) {
      LOG.error(Utils.format("An error occurred while running the pipeline, {}", e.getMessage()));
      throw e;
    }
  }

  public void stop() {
    pipeline.stop();
  }

  public String getRev() {
    return rev;
  }
}
