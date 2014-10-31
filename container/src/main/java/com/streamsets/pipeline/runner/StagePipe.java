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
import com.streamsets.pipeline.container.Configuration;

import java.util.Collections;
import java.util.List;

public class StagePipe extends Pipe {

  public StagePipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes) {
    super(stage, inputLanes, outputLanes);
  }

  @Override
  public void reconfigure(Configuration conf) {
  }

  @Override
  public void init() throws StageException {
    getStage().init();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(PipeBatch pipeBatch) throws StageException, PipelineRuntimeException {
    switch (getStage().getDefinition().getType()) {
      case SOURCE:
        Source.Context sourceContext = getStage().getContext();
        pipeBatch.configure(sourceContext.getOutputLanes());
        String newOffset = ((Source) getStage().getStage()).produce(pipeBatch.getPreviousOffset(),
                                                               pipeBatch.getBatchMaker());
        pipeBatch.setNewOffset(newOffset);
        break;
      case PROCESSOR:
        Processor.Context processorContext = getStage().getContext();
        pipeBatch.configure(processorContext.getOutputLanes());
        ((Processor) getStage().getStage()).process(pipeBatch.getBatch(), pipeBatch.getBatchMaker());
        break;
      case TARGET:
        pipeBatch.configure(Collections.EMPTY_SET);
        ((Target) getStage().getStage()).write(pipeBatch.getBatch());
        break;
    }
    pipeBatch.flip();
  }

  @Override
  public void destroy() {
    getStage().destroy();
  }


}
