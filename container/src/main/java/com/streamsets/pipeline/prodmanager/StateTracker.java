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
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.io.FilenameFilter;
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
  public static final String STATE = "state";

  private final File stateDir;
  private final RuntimeInfo runtimeInfo;
  private volatile PipelineState pipelineState;
  private final com.streamsets.pipeline.util.Configuration configuration;

  public StateTracker(RuntimeInfo runtimeInfo, com.streamsets.pipeline.util.Configuration configuration) {
    Preconditions.checkNotNull(runtimeInfo, "runtime info cannot be null");
    stateDir = new File(runtimeInfo.getDataDir(), STATE_DIR);
    this.configuration = configuration;
    this.runtimeInfo = runtimeInfo;
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

  public synchronized void setState(String name, String rev, State state, String message)
    throws PipelineManagerException {
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
      }
    } else {
      //There exists a pipelineState directory already, check for file
      if(getStateFile().exists()) {
        try {
          this.pipelineState = getPersistedState();
        } catch (PipelineManagerException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public void register(String pipelineName, String rev) {
    LogUtil.registerLogger(pipelineName, rev, STATE, getPipelineStateFile(pipelineName, rev).getAbsolutePath(),
      configuration);
  }

  private PipelineState getPersistedState() throws PipelineManagerException {
    try {
      return (PipelineState)JsonFileUtil.readObjectFromFile(getStateFile(), PipelineState.class);
    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0101.getMessage(), e.getMessage());
      throw new PipelineManagerException(ContainerError.CONTAINER_0101, e.getMessage(), e);
    }
  }

  private void persistPipelineState(PipelineState pipelineState) throws PipelineManagerException {
    //write to runInfo/pipelineState.json as well as /runInfo/<pipelineName>/pipelineState.json
    try {
      JsonFileUtil.writeObjectToFile(getTempStateFile(), getStateFile(), pipelineState);

      //In addition, append the state of the pipeline to the pipelineState.json present in the directory of that
      //pipeline
      LogUtil.log(pipelineState.getName(), pipelineState.getRev(), STATE,
        ObjectMapperFactory.get().writeValueAsString(pipelineState));

    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0100.getMessage(), e.getMessage());
      throw new PipelineManagerException(ContainerError.CONTAINER_0100, e.getMessage(), e);
    }
  }

  public File getPipelineStateFile(String name, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, name, rev), STATE_FILE);
  }

  @SuppressWarnings("unchecked")
  public List<PipelineState> getHistory(String pipelineName, String rev) {
    if(!pipelineDirExists(pipelineName, rev) || !pipelineStateFileExists(pipelineName, rev)) {
      return Collections.emptyList();
    }
    try {
      Reader reader = new FileReader(getPipelineStateFile(pipelineName, rev));
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

  private boolean pipelineStateFileExists(String pipelineName, String rev) {
    return getPipelineStateFile(pipelineName, rev).exists();
  }

  private boolean pipelineDirExists(String pipelineName, String rev) {
    return PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists();
  }

  public void deleteHistory(String pipelineName, String rev) {
    for(File f : getStateFiles(pipelineName, rev)) {
      f.delete();
    }
    LogUtil.resetRollingFileAppender(pipelineName, rev, STATE, getPipelineStateFile(pipelineName, rev)
      .getAbsolutePath(), configuration);
  }

  private File[] getStateFiles(String pipelineName, String rev) {
    //RollingFileAppender creates backup files when the error files reach the size limit.
    //The backup files are of the form errors.json.1, errors.json.2 etc
    //Need to delete all the backup files
    File pipelineDir = PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev);
    File[] errorFiles = pipelineDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if(name.contains(STATE_FILE)) {
          return true;
        }
        return false;
      }
    });
    return errorFiles;
  }

}
