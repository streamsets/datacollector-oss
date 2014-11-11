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

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.state.PipelineStateException;
import com.streamsets.pipeline.state.PipelineStateManager;
import com.streamsets.pipeline.state.State;

public class ProdPipelineRunnerThread implements Runnable {

  private final PipelineStateManager pipelineStateManager;
  private final ProductionPipeline pipeline;

  public ProdPipelineRunnerThread(PipelineStateManager pipelineStateManager, ProductionPipeline pipeline) {
    this.pipelineStateManager = pipelineStateManager;
    this.pipeline = pipeline;
  }

  @Override
  public void run() {
    try {
      pipeline.run();
      //finished running pipeline without errors.
      pipelineStateManager.setState(State.NOT_RUNNING);
    } catch (StageException e) {
      try {
        pipelineStateManager.setState(State.ERROR);
      } catch (PipelineStateException ex) {
        throw new RuntimeException(ex);
      }
    } catch (PipelineRuntimeException e) {
      try {
        pipelineStateManager.setState(State.ERROR);
      } catch (PipelineStateException ex) {
        throw new RuntimeException(ex);
      }
    } catch (PipelineStateException e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    ((ProductionPipelineRunner)pipeline.getPipeline().getRunner()).stop();
  }

}
