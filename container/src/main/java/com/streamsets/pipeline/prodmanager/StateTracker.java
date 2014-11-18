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
package com.streamsets.pipeline.prodmanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.main.RuntimeInfo;

import java.io.File;
import java.io.IOException;

public class StateTracker {

  static final String DEFAULT_PIPELINE_NAME = "xyz";
  private static final String STATE_FILE = "pipelineState.json";
  private static final String STATE_DIR = "runInfo";
  private static final String DEFAULT_STATE = "NOT_RUNNING";
  private static final String DEFAULT_PIPELINE_REVISION = "1.0";

  private File stateDir;
  private PipelineState pipelineState;
  private final ObjectMapper json;
  private final RuntimeInfo runtimeInfo;

  public StateTracker(RuntimeInfo runtimeInfo) {
    json = new ObjectMapper();
    json.enable(SerializationFeature.INDENT_OUTPUT);
    this.runtimeInfo = runtimeInfo;
  }

  public PipelineState getState() {
    return this.pipelineState;
  }

  @VisibleForTesting
  public File getStateFile() {
    return new File(stateDir, STATE_FILE);
  }

  public void setState(String rev, State state, String message) throws PipelineStateException {
    //persist default pipelineState
    pipelineState = new PipelineState(rev, state, message, System.currentTimeMillis());
    try {
      json.writeValue(getStateFile(), pipelineState);
    } catch (IOException e) {
      throw new PipelineStateException(PipelineStateException.ERROR.COULD_NOT_SET_STATE, e.getMessage(), e);
    }
  }

  public void init() {
    stateDir = new File(runtimeInfo.getDataDir(), "runInfo" + File.separator + DEFAULT_PIPELINE_NAME);
    if (!stateDir.exists()) {
      if (!stateDir.mkdirs()) {
        throw new RuntimeException(String.format("Could not create directory '%s'", stateDir.getAbsolutePath()));
      } else {
        persistDefaultState();
      }
    } else {
      //There exists a pipelineState directory already, check for file
      if(getStateFile().exists()) {
        try {
          this.pipelineState = getStateFromDir();
        } catch (PipelineStateException e) {
          throw new RuntimeException(e);
        }
      } else {
        persistDefaultState();
      }
    }
  }

  private void persistDefaultState() {
    //persist default pipelineState
    try {
      setState(DEFAULT_PIPELINE_REVISION, State.NOT_RUNNING, null);
    } catch (PipelineStateException e) {
      throw new RuntimeException(e);
    }
  }

  private PipelineState getStateFromDir() throws PipelineStateException {
    try {
      return json.readValue(getStateFile(), PipelineState.class);
    } catch (IOException e) {
      throw new PipelineStateException(PipelineStateException.ERROR.COULD_NOT_GET_STATE, e.getMessage(), e);
    }
  }


}
