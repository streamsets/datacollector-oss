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
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.validation.Issue;

import java.util.Collections;
import java.util.List;

public class ObserverPipe extends Pipe<Pipe.Context> {
  private final Observer observer;

  public ObserverPipe(StageRuntime stage, List<String> inputLanes, List<String> outputLanes, Observer observer) {
    super(stage, inputLanes, outputLanes, Collections.<String>emptyList());
    this.observer = observer;
  }

  @Override
  public List<Issue> init(Pipe.Context pipeContext) {
    return Collections.emptyList();
  }

  @Override
  public void destroy(PipeBatch pipeBatch) {
  }

  @Override
  public void process(PipeBatch pipeBatch) throws PipelineRuntimeException {
    if (observer != null && observer.isObserving(getInputLanes())) {
      observer.observe(this, pipeBatch.getLaneOutputRecords(getInputLanes()));
    }
    for (int i = 0; i < getInputLanes().size(); i++) {
      pipeBatch.moveLane(getInputLanes().get(i), getOutputLanes().get(i));
    }
  }

}
