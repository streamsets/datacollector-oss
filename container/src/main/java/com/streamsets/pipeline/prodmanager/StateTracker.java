/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.dc.execution.StateEventListener;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.PipelineStateJson;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

  private final List<StateEventListener> stateEventListenerList = new ArrayList<>();

  public void addStateEventListener(StateEventListener stateListener) {
    stateEventListenerList.add(stateListener);
  }

  public void removeStateEventListener(StateEventListener stateListener) {
    stateEventListenerList.remove(stateListener);
  }

  public StateTracker(RuntimeInfo runtimeInfo, com.streamsets.pipeline.util.Configuration configuration) {
    Preconditions.checkNotNull(runtimeInfo, "runtime info cannot be null");
    stateDir = new File(runtimeInfo.getDataDir(), STATE_DIR);
    if (!stateDir.exists()) {
      if (!stateDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", stateDir));
      }
    }
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

  public synchronized void setState(String name, String rev, State state, String message, MetricRegistry metricRegistry,
      Map<String, Object> attributes)
    throws PipelineManagerException {
    //Need to persist the state first and then update the pipeline state field.
    //Otherwise looking up the history after stopping the pipeline may or may not show the last STOPPED state

    String metricRegistryStr = null;

    if(metricRegistry != null) {
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      try {
        metricRegistryStr = objectMapper.writer().writeValueAsString(metricRegistry);
      } catch (JsonProcessingException e) {
        String msg = "Error serializing metric registry: " + e;
        LOG.error(msg, e);
      }
    }

    PipelineState tempPipelineState = new PipelineState(name, rev, state, message, System.currentTimeMillis(),
                                                        metricRegistryStr, attributes);
    if(state != State.STOPPING) {
      persistPipelineState(tempPipelineState);
    }
    pipelineState = tempPipelineState;

    if(stateEventListenerList.size() > 0) {
      try {
        ObjectMapper objectMapper = ObjectMapperFactory.get();
        String pipelineStateJSONStr = objectMapper.writer().writeValueAsString(pipelineState);

        for(StateEventListener stateEventListener : stateEventListenerList) {
          try {
            stateEventListener.notification(pipelineStateJSONStr);
          } catch(Exception ex) {
            LOG.warn("Error while notifying metrics, {}", ex.getMessage(), ex);
          }
        }
      } catch (JsonProcessingException ex) {
        LOG.warn("Error while broadcasting Pipeline State, {}", ex.getMessage(), ex);
      }
    }
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
    try (InputStream is = new DataStore(getStateFile()).getInputStream()){
      PipelineStateJson pipelineStateJsonBean = ObjectMapperFactory.get().readValue(is, PipelineStateJson.class);
      return pipelineStateJsonBean.getPipelineState();
    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0101.getMessage(), e.getMessage(), e);
      throw new PipelineManagerException(ContainerError.CONTAINER_0101, e.getMessage(), e);
    }
  }

  private void persistPipelineState(PipelineState pipelineState) throws PipelineManagerException {
    //write to runInfo/pipelineState.json as well as /runInfo/<pipelineName>/pipelineState.json
    try (OutputStream os = new DataStore(getStateFile()).getOutputStream()){
      ObjectMapperFactory.get().writeValue(os, BeanHelper.wrapPipelineState(pipelineState));
      //In addition, append the state of the pipeline to the pipelineState.json present in the directory of that
      //pipeline
      LogUtil.log(pipelineState.getName(), pipelineState.getRev(), STATE,
        ObjectMapperFactory.get().writeValueAsString(BeanHelper.wrapPipelineState(pipelineState)));
    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0100.getMessage(), e.getMessage(), e);
      throw new PipelineManagerException(ContainerError.CONTAINER_0100, e.getMessage(), e);
    }
  }

  public File getPipelineStateFile(String name, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, name, rev), STATE_FILE);
  }

  @SuppressWarnings("unchecked")
  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) {
    if(!pipelineDirExists(pipelineName, rev) || !pipelineStateFileExists(pipelineName, rev)) {
      return Collections.emptyList();
    }
    try {
      Reader reader = new FileReader(getPipelineStateFile(pipelineName, rev));
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      JsonParser jsonParser = objectMapper.getFactory().createParser(reader);
      MappingIterator<PipelineStateJson> pipelineStateMappingIterator =
        objectMapper.readValues(jsonParser, PipelineStateJson.class);
      List<PipelineStateJson> pipelineStateJsons =  pipelineStateMappingIterator.readAll();
      Collections.reverse(pipelineStateJsons);
      if(fromBeginning) {
        return BeanHelper.unwrapPipelineStates(pipelineStateJsons);
      } else {
        int toIndex = pipelineStateJsons.size() > 100 ? 100 : pipelineStateJsons.size();
        return BeanHelper.unwrapPipelineStates(pipelineStateJsons.subList(0,toIndex));
      }
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
    LogUtil.resetRollingFileAppender(pipelineName, rev, STATE);
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
