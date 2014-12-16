/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.JsonFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.io.Reader;
import java.util.Collections;
import java.io.File;
import java.io.IOException;
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
    Preconditions.checkNotNull(runtimeInfo, "runtime info cannot be null");
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

  public void setState(String name, String rev, State state, String message) throws PipelineManagerException {
    //Need to persist the state first and then update the pipeline state field.
    //Otherwise looking up the history after stopping the pipeline may or may not show the last STOPPED state
    PipelineState tempPipelineState = new PipelineState(name, rev, state, message, System.currentTimeMillis());
    if(state != State.STOPPING) {
      persistPipelineState(tempPipelineState);
    }
    pipelineState = tempPipelineState;
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
        } catch (PipelineManagerException e) {
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
    } catch (PipelineManagerException e) {
      throw new RuntimeException(e);
    }
  }

  private PipelineState getStateFromDir() throws PipelineManagerException {
    try {
      return json.readObjectFromFile(getStateFile(), PipelineState.class);
    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0101.getMessage(), e.getMessage());
      throw new PipelineManagerException(ContainerError.CONTAINER_0101, e.getMessage(), e);
    }
  }

  private void persistPipelineState(PipelineState pipelineState) throws PipelineManagerException {
    //write to runInfo/pipelineState.json as well as /runInfo/<pipelineName>/pipelineState.json
    try {
      json.writeObjectToFile(getTempStateFile(), getStateFile(), pipelineState);

      //In addition, append the state of the pipeline to the pipelineState.json present in the directory of that
      //pipeline
      File pipelineStateTempFile = getPipelineStateTempFile(pipelineState.getName());
      File pipelineStateFile = getPipelineStateFile(pipelineState.getName());
      json.appendObjectToFile(pipelineStateTempFile, pipelineStateFile, pipelineState);

    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0100.getMessage(), e.getMessage());
      throw new PipelineManagerException(ContainerError.CONTAINER_0100, e.getMessage(), e);
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

  @SuppressWarnings("unchecked")
  public List<PipelineState> getHistory(String pipelineName) {
    if(!doesPipelineDirExist(pipelineName)) {
      return Collections.EMPTY_LIST;
    }
    try {
      Reader reader = new FileReader(getPipelineStateFile(pipelineName));
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      JsonParser jsonParser = objectMapper.getFactory().createParser(reader);
      MappingIterator<PipelineState> pipelineStateMappingIterator = objectMapper.readValues(jsonParser,
                                                                                            PipelineState.class);
      List<PipelineState> pipelineStates =  pipelineStateMappingIterator.readAll();
      Collections.reverse(pipelineStates);
      return pipelineStates;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean doesPipelineDirExist(String pipelineName) {
    File pipelineDir = new File(stateDir, pipelineName);
    return pipelineDir.exists();
  }

}
