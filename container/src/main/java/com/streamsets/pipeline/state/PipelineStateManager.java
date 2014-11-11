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
package com.streamsets.pipeline.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.runner.production.ProdPipelineRunnerThread;
import com.streamsets.pipeline.runner.production.ProductionPipeline;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executors;


public class PipelineStateManager {

  static final String DEFAULT_PIPELINE_NAME = "xyz";
  private static final String PIPELINE_STATE_FILE = "state.json";

  private final RuntimeInfo runtimeInfo;
  private File stateDir;
  private volatile State state;
  private ObjectMapper json;
  private ProdPipelineRunnerThread t;

  @Inject
  public PipelineStateManager(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
  }

  public State getState() {
    return this.state;
  }

  public void setState(State state) throws PipelineStateException {
    //persist default state
    PipelineState ps = new PipelineState(DEFAULT_PIPELINE_NAME, state, Collections.EMPTY_LIST);
    try {
      json.writeValue(getStateFile(), ps);
      this.state = state;
    } catch (IOException e) {
      //TODO localize
      throw new PipelineStateException(PipelineStateErrors.COULD_NOT_SET_STATE, e.getMessage(), e);
    }
  }

  public void init() {
    stateDir = new File(runtimeInfo.getDataDir(), "runInfo" + File.separator + DEFAULT_PIPELINE_NAME);
    if (!stateDir.exists()) {
      if (!stateDir.mkdirs()) {
        throw new RuntimeException(String.format("Could not create directory '%s'", stateDir.getAbsolutePath()));
      } else {
        //persist default state
        try {
          setState(State.NOT_RUNNING);
        } catch (PipelineStateException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      //There exists a state directory already, read state from it.
      try {
        this.state = getStateFromDir();
      } catch (PipelineStateException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void destroy() {

  }

  private File getStateFile() {
    return new File(stateDir, PIPELINE_STATE_FILE);
  }

  private State getStateFromDir() throws PipelineStateException {
    try {
      PipelineState pipelineState = json.readValue(getStateFile(), PipelineState.class);
      return pipelineState.getPipelineState();
    } catch (IOException e) {
      //TODO localize
      throw new PipelineStateException(PipelineStateErrors.COULD_NOT_GET_STATE, e.getMessage(), e);
    }
  }

  public void runPipeline(ProductionPipeline pipeline) {
    t = new ProdPipelineRunnerThread(this, pipeline);
    Executors.newSingleThreadExecutor().submit(t);
  }

  public void stopPipeline() {
    t.stop();
  }
}
