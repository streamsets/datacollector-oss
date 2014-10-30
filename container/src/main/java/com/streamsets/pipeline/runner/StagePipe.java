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
package com.streamsets.pipeline.runner;

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import java.util.Collections;

public class StagePipe extends Pipe {
  private final StageRuntime stage;

  public StagePipe(StageRuntime stage) {
    this.stage = stage;
  }

  @Override
  public void init() throws StageException {
    stage.init();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    switch (stage.getDefinition().getType()) {
      case SOURCE:
        Source.Context sourceContext = stage.getContext();
        pipeBatch.configure(sourceContext.getOutputLanes());
        String newOffset = ((Source) stage.getStage()).produce(pipeBatch.getPreviousOffset(),
                                                               pipeBatch.getBatchMaker());
        pipeBatch.setNewOffset(newOffset);
        break;
      case PROCESSOR:
        Processor.Context processorContext = stage.getContext();
        pipeBatch.configure(processorContext.getOutputLanes());
        ((Processor) stage.getStage()).process(pipeBatch.getBatch(), pipeBatch.getBatchMaker());
        break;
      case TARGET:
        Target.Context targetContext = stage.getContext();
        pipeBatch.configure(Collections.EMPTY_SET);
        ((Target) stage.getStage()).write(pipeBatch.getBatch());
        break;
    }
    pipeBatch.flip();
  }

  @Override
  public void destroy() {
    stage.destroy();
  }

}
