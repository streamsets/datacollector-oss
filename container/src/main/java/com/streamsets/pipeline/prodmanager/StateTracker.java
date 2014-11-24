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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.util.JsonFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.util.ArrayList;
import java.util.List;

public class StateTracker {

  private static final Logger LOG = LoggerFactory.getLogger(StateTracker.class);

  public static final String STATE_FILE = "pipelineState.json";
  public static final String TEMP_STATE_FILE = "pipelineState.json.tmp";
  public static final String STATE_DIR = "runInfo";

  private File stateDir;
  private volatile PipelineState pipelineState;
  private final JsonFileUtil<PipelineState> json;

  public StateTracker(RuntimeInfo runtimeInfo) {
    Preconditions.checkArgument(runtimeInfo != null);
    stateDir = new File(runtimeInfo.getDataDir(), STATE_DIR);
    json = new JsonFileUtil<>();
  }

  public PipelineState getState() {
    return this.pipelineState;
  }

  @VisibleForTesting
  public File getStateFile() {
    return new File(stateDir, STATE_FILE);
  }

  @VisibleForTesting
  File getTempStateFile() {
    return new File(stateDir, TEMP_STATE_FILE);
  }

  public void setState(String name, String rev, State state, String message) throws PipelineStateException {
    //persist default pipelineState
    pipelineState = new PipelineState(name, rev, state, message, System.currentTimeMillis());
    if(state != State.STOPPING) {
      persistPipelineState();
    }
  }

  public void init() {
    if (!stateDir.exists()) {
      if (!stateDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory {}", stateDir.getAbsolutePath()));
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
      setState(Constants.DEFAULT_PIPELINE_NAME, Constants.DEFAULT_PIPELINE_REVISION, State.STOPPED, null);
    } catch (PipelineStateException e) {
      throw new RuntimeException(e);
    }
  }

  private PipelineState getStateFromDir() throws PipelineStateException {
    try {
      return json.readObjectFromFile(getStateFile(), PipelineState.class);
    } catch (IOException e) {
      LOG.error(PipelineStateException.ERROR.COULD_NOT_GET_STATE.getMessageTemplate(), e.getMessage());
      throw new PipelineStateException(PipelineStateException.ERROR.COULD_NOT_GET_STATE, e.getMessage(), e);
    }
  }

  private void persistPipelineState() throws PipelineStateException {
    //write to runInfo/pipelineState.json as well as /runInfo/<pipelineName>/pipelineState.json
    try {
      json.writeObjectToFile(getTempStateFile(), getStateFile(), pipelineState);

      //In addition, append the state of the pipeline to the pipelineState.json present in the directory of that
      //pipeline
      File pipelineStateTempFile = getPipelineStateTempFile(pipelineState.getName());
      File pipelineStateFile = getPipelineStateFile(pipelineState.getName());
      json.appendObjectToFile(pipelineStateTempFile, pipelineStateFile, pipelineState);

    } catch (IOException e) {
      LOG.error(PipelineStateException.ERROR.COULD_NOT_SET_STATE.getMessageTemplate(), e.getMessage());
      throw new PipelineStateException(PipelineStateException.ERROR.COULD_NOT_SET_STATE, e.getMessage(), e);
    }
  }

  public File getPipelineStateFile(String name) {
    return new File(getPipelineDir(name), STATE_FILE);
  }

  private File getPipelineStateTempFile(String name) {
    return new File(getPipelineDir(name), TEMP_STATE_FILE);
  }

  private File getPipelineDir(String name) {
    File pipelineDir = new File(stateDir, name);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        LOG.error("Could not create directory '{}'", pipelineDir.getAbsolutePath());
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
    return pipelineDir;
  }

  public List<PipelineState> getHistory(String pipelineName) {
    PipelineState[] pipelineStates = null;
    try {
      pipelineStates = new ObjectMapper().readValue(getPipelineStateFile(pipelineName), PipelineState[].class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    List<PipelineState> pipelineStateList = new ArrayList<>(pipelineStates.length);
    for(int i = pipelineStates.length - 1; i >= 0; i--) {
      pipelineStateList.add(pipelineStates[i]);
    }
    return pipelineStateList;
  }

}
