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
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;

public class ProductionPipeline {
  private final Pipeline pipeline;
  private final ProductionPipelineRunner pipelineRunner;

  public ProductionPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
    this.pipelineRunner =  (ProductionPipelineRunner)pipeline.getRunner();
  }

  public void run() throws StageException, PipelineRuntimeException{
    pipeline.init();
    try {
      pipeline.run();
    } finally {
      pipeline.destroy();
    }
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  public void stop() {
    pipelineRunner.stop();
  }

  public boolean wasStopped() {
    return pipelineRunner.wasStopped();
  }

  public String getCommittedOffset() {
    return pipelineRunner.getCommittedOffset();
  }

  public void captureSnapshot(int batchSize) {
    pipelineRunner.captureNextBatch(batchSize);
  }

  public void setOffset(String offset) {
    ProductionSourceOffsetTracker offsetTracker = (ProductionSourceOffsetTracker) pipelineRunner.getOffSetTracker();
    offsetTracker.setOffset(offset);
    offsetTracker.commitOffset();
  }
}
