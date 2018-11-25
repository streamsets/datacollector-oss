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
package com.streamsets.datacollector.execution.store;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LogUtil;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilePipelineStateStore implements PipelineStateStore {
  private final RuntimeInfo runtimeInfo;
  private final Configuration configuration;
  public static final String STATE_FILE = "pipelineState.json";
  public static final String STATE_FILE_HISTORY = "pipelineStateHistory.json";
  public static final String STATE = "state";
  private static final Logger LOG = LoggerFactory.getLogger(FilePipelineStateStore.class);

  @Inject
  public FilePipelineStateStore(RuntimeInfo runtimeInfo, Configuration conf) {
    this.runtimeInfo = runtimeInfo;
    this.configuration = conf;
    File stateDir = new File(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_BASE_DIR);
    if (!(stateDir.exists() || stateDir.mkdirs()) || !stateDir.isDirectory()) {
      throw new RuntimeException(Utils.format("Could not create directory '{}'", stateDir));
    }
  }

  @Override
  public void init() {
  }

  @Override
  public void destroy() {
  }

  @Override
  public PipelineState edited(
      String user, String name, String rev, ExecutionMode executionMode, boolean isRemote, Map<String, Object> metadata
  ) throws PipelineStoreException {
    PipelineState pipelineState = null;
    if (getPipelineStateFile(name, rev).exists()) {
      pipelineState = getState(name, rev);
      Utils.checkState(!pipelineState.getStatus().isActive(),
        Utils.format("Cannot edit pipeline in state: '{}'", pipelineState.getStatus()));
    }
    // first time when pipeline is created
    Map<String, Object> attributes = null;
    if (pipelineState == null) {
      attributes = new HashMap<>();
      attributes.put(RemoteDataCollector.IS_REMOTE_PIPELINE, isRemote);
      if (metadata!=null) {
        attributes.putAll(metadata);
      }
    }
    if (pipelineState == null
      || pipelineState.getStatus() != PipelineStatus.EDITED
      || executionMode != pipelineState.getExecutionMode()
      ) {
      return saveState(user, name, rev, PipelineStatus.EDITED, "Pipeline edited", attributes, executionMode, null, 0, 0);
    } else {
      return null;
    }
  }

  private String getNameAndRevString(String name, String rev) {
    return name + "::" + rev;
  }

  @Override
  public void delete(String name, String rev) {
    File pipelineStateFile = getPipelineStateFile(name, rev);
    FileUtils.deleteQuietly(pipelineStateFile);
  }

  @Override
  public PipelineState saveState(String user, String name, String rev, PipelineStatus status, String message,
    Map<String, Object> attributes, ExecutionMode executionMode, String metrics, int retryAttempt, long nextRetryTimeStamp
   )
    throws PipelineStoreException {
    register(name, rev);
    LOG.debug("Changing state of pipeline '{}','{}','{}' to '{}' in execution mode: '{}';" + "status msg is '{}'",
      name, rev, user, status, executionMode, message);
    if (attributes == null && getPipelineStateFile(name, rev).exists()) {
      attributes = getState(name, rev).getAttributes();
      if (attributes.containsKey("issues")) {
        attributes.remove("issues");
      }
    }
    PipelineState pipelineState =
      new PipelineStateImpl(user, name, rev, status, message, System.currentTimeMillis(), attributes, executionMode,
        metrics, retryAttempt, nextRetryTimeStamp);
    persistPipelineState(pipelineState);
    return pipelineState;
  }

  private PipelineState loadState(String nameAndRev) throws PipelineStoreException {
    PipelineState pipelineState = null;
    String[] nameAndRevArray = nameAndRev.split("::");
    String name = nameAndRevArray[0];
    String rev = nameAndRevArray[1];
    LOG.debug("Loading state from file for pipeline: '{}'::'{}'", name, rev);
    try {
      // SDC-2930: We don't check for the existence of the actual state file itself, since the DataStore will take care of
      // picking up the correct files (the new/tmp files etc).
      DataStore ds = new DataStore(getPipelineStateFile(name, rev));
      if (ds.exists()) {
        try (InputStream is = ds.getInputStream()) {
          PipelineStateJson pipelineStatusJsonBean = ObjectMapperFactory.get().readValue(is, PipelineStateJson.class);
          pipelineState = pipelineStatusJsonBean.getPipelineState();
        }
      } else {
        throw new PipelineStoreException(ContainerError.CONTAINER_0209, getPipelineStateFile(name, rev));
      }
    } catch (IOException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0101, e.toString(), e);
    }
    return pipelineState;
  }

  @Override
  public PipelineState getState(String name, String rev) throws PipelineStoreException {
      return loadState(getNameAndRevString(name, rev));
  }

  @Override
  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) throws PipelineStoreException {
    if (!pipelineDirExists(pipelineName, rev) || !pipelineStateHistoryFileExists(pipelineName, rev)) {
      return Collections.emptyList();
    }
    try (Reader reader = new FileReader(getPipelineStateHistoryFile(pipelineName, rev))){
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      JsonParser jsonParser = objectMapper.getFactory().createParser(reader);
      MappingIterator<PipelineStateJson> pipelineStateMappingIterator =
        objectMapper.readValues(jsonParser, PipelineStateJson.class);
      List<PipelineStateJson> pipelineStateJsons = pipelineStateMappingIterator.readAll();
      Collections.reverse(pipelineStateJsons);
      if (fromBeginning) {
        return BeanHelper.unwrapPipelineStatesNewAPI(pipelineStateJsons);
      } else {
        int toIndex = pipelineStateJsons.size() > 100 ? 100 : pipelineStateJsons.size();
        return BeanHelper.unwrapPipelineStatesNewAPI(pipelineStateJsons.subList(0, toIndex));
      }
    } catch (IOException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0115, pipelineName, rev, e.toString(), e);
    }
  }

  @Override
  public void deleteHistory(String pipelineName, String rev) {
    LogUtil.resetRollingFileAppender(pipelineName, rev, STATE);
    for (File f : getHistoryStateFiles(pipelineName, rev)) {
      if (!f.delete()) {
        LOG.warn("Failed to delete history file " + f);
      }
    }
  }

  private void register(String pipelineName, String rev) {
    LogUtil.registerLogger(pipelineName, rev, STATE, getPipelineStateHistoryFile(pipelineName, rev).getAbsolutePath(),
      configuration);
  }

  private void persistPipelineState(PipelineState pipelineState) throws PipelineStoreException {
    // write to /runInfo/<pipelineName>/pipelineState.json as well as /runInfo/<pipelineName>/pipelineStateHistory.json
    PipelineStateJson pipelineStateJson = BeanHelper.wrapPipelineState(pipelineState);
    String pipelineString;
    try {
      pipelineString = ObjectMapperFactory.get().writeValueAsString(pipelineStateJson);
    } catch (JsonProcessingException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0210, e.toString(), e);
    }
    DataStore dataStore = new DataStore(getPipelineStateFile(pipelineState.getPipelineId(), pipelineState.getRev()));
    try (OutputStream os = dataStore.getOutputStream()) {
      os.write(pipelineString.getBytes());
      dataStore.commit(os);
    } catch (IOException e) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0100, e.toString(), e);
    } finally {
      dataStore.release();
    }
    // In addition, append the state of the pipeline to the pipelineState.json present in the directory of that
    // pipeline
    LogUtil.log(pipelineState.getPipelineId(), pipelineState.getRev(), STATE, pipelineString);
  }

  private File[] getHistoryStateFiles(String pipelineName, String rev) {
    // RollingFileAppender creates backup files when the error files reach the size limit.
    // The backup files are of the form errors.json.1, errors.json.2 etc
    // Need to delete all the backup files
    File pipelineDir = PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev);
    File[] errorFiles = pipelineDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.contains(STATE_FILE_HISTORY)) {
          return true;
        }
        return false;
      }
    });
    return errorFiles;
  }

  private File getPipelineStateFile(String name, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, name, rev), STATE_FILE);
  }

  private File getPipelineStateHistoryFile(String name, String rev) {
    return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, name, rev), STATE_FILE_HISTORY);
  }

  private boolean pipelineDirExists(String pipelineName, String rev) {
    return PipelineDirectoryUtil.getPipelineDir(runtimeInfo, pipelineName, rev).exists();
  }

  private boolean pipelineStateHistoryFileExists(String pipelineName, String rev) {
    return getPipelineStateHistoryFile(pipelineName, rev).exists();
  }

}
