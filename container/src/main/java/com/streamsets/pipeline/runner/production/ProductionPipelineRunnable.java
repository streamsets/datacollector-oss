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
import com.streamsets.pipeline.state.PipelineStateException;
import com.streamsets.pipeline.state.PipelineManager;
import com.streamsets.pipeline.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class ProductionPipelineRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunnable.class);

  private final PipelineManager pipelineManager;
  private final ProductionPipeline pipeline;

  public ProductionPipelineRunnable(PipelineManager pipelineManager, ProductionPipeline pipeline) {
    this.pipelineManager = pipelineManager;
    this.pipeline = pipeline;
  }

  @Override
  public void run() {
    try {
      pipeline.run();
      //finished running pipeline without errors.
      if(pipeline.wasStopped()) {
        pipelineManager.setState(State.NOT_RUNNING,
            Utils.format("The pipeline was stopped. The last committed source offset is {}.",
                pipeline.getCommittedOffset()));
      } else {
        pipelineManager.setState(State.NOT_RUNNING, "Completed successfully.");
      }
    } catch (Exception e) {
      LOG.error(Utils.format("An exception occurred while running the pipeline, {}", e.getMessage()));
      try {
        pipelineManager.setState(State.ERROR, e.getMessage());
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

}
