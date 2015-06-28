/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.store;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.manager.PipelineStateImpl;
import com.streamsets.dataCollector.execution.util.PipelineStatusUtil;
import com.streamsets.dataCollector.restapi.bean.PipelineStateJson;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.LogUtil;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;

public class FilePipelineStateStore implements PipelineStateStore {
  private RuntimeInfo runtimeInfo;
  private Configuration configuration;
  public static final String STATE_FILE = "pipelineState.json";
  public static final String STATE_FILE_HISTORY = "pipelineStateHistory.json";
  public static final String STATE = "state";
  private static final Logger LOG = LoggerFactory.getLogger(FilePipelineStateStore.class);
  private LoadingCache<String, PipelineState> pipelineStateCache;

  @Inject
  public FilePipelineStateStore(RuntimeInfo runtimeInfo, Configuration conf) {
    this.runtimeInfo = runtimeInfo;
    this.configuration = conf;
    File stateDir = new File(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_BASE_DIR);
    if (!stateDir.exists()) {
      if (!stateDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", stateDir));
      }
    }
  }

  @Override
  public void init() {
    pipelineStateCache = CacheBuilder.newBuilder().
      maximumSize(100).
      expireAfterAccess(10, TimeUnit.MINUTES).
      build(new CacheLoader<String, PipelineState>() {
      @Override
      public PipelineState load(String nameAndRev) throws Exception {
        return loadState(nameAndRev);
      }
    });
  }

  @Override
  public void destroy() {
    pipelineStateCache.invalidateAll();
  }

  @Override
  public void edited(String user, String name, String rev, ExecutionMode executionMode) throws PipelineStoreException {
    PipelineState pipelineState = getState(name, rev);
    Utils.checkState(!PipelineStatusUtil.isActive(pipelineState.getStatus()), "Cannot edit pipeline in state: " + pipelineState.getStatus());
    if (pipelineState.getStatus() != PipelineStatus.EDITED || executionMode != pipelineState.getExecutionMode()) {
      saveState(user, name, rev, PipelineStatus.EDITED, "Pipeline edited", null, executionMode);
    }
  }

  private String getNameAndRevString(String name, String rev) {
    return name + "::" + rev;
  }

  @Override
  public void delete(String name, String rev) {
    getPipelineStateFile(name, rev).delete();
    pipelineStateCache.invalidate(getNameAndRevString(name, rev));
  }

  @Override
  public void saveState(String user, String name, String rev, PipelineStatus status, String message,
    Map<String, Object> attributes, ExecutionMode executionMode) throws PipelineStoreException {
    register(name, rev);
    LOG.debug("Changing state of pipeline '{}','{}','{}' to '{}' in execution mode: '{}'", name, rev, user, status,
      executionMode);
    PipelineState pipelineState =
      new PipelineStateImpl(name, rev, user, status, message, System.currentTimeMillis(), attributes, executionMode);
    persistPipelineState(pipelineState);
    pipelineStateCache.put(getNameAndRevString(name, rev), pipelineState);
  }

  private PipelineState loadState(String nameAndRev) throws PipelineStoreException {
    PipelineState pipelineState = null;
    try {
      String[] nameAndRevArray = nameAndRev.split("::");
      String name = nameAndRevArray[0];
      String rev = nameAndRevArray[1];
      LOG.debug("Loading state from file for pipeline " + name + " and rev " + rev);
      if (getPipelineStateFile(name, rev).exists()) {
        com.streamsets.dataCollector.restapi.bean.PipelineStateJson pipelineStatusJsonBean =
          ObjectMapperFactory.get().readValue(new DataStore(getPipelineStateFile(name, rev)).getInputStream(),
            com.streamsets.dataCollector.restapi.bean.PipelineStateJson.class);
        pipelineState = pipelineStatusJsonBean.getPipelineState();
      } else {
        throw new PipelineStoreException(ContainerError.CONTAINER_0209, getPipelineStateFile(name, rev));
      }
    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0101.getMessage(), e.getMessage(), e);
      throw new PipelineStoreException(ContainerError.CONTAINER_0101, e.getMessage(), e);
    }
    return pipelineState;
  }

  @Override
  public PipelineState getState(String name, String rev) throws PipelineStoreException {
    try {
      return pipelineStateCache.get(getNameAndRevString(name, rev));
    } catch (ExecutionException ex) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0114, ex.getMessage(), ex);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PipelineState> getHistory(String pipelineName, String rev, boolean fromBeginning) {
    if (!pipelineDirExists(pipelineName, rev) || !pipelineStateHistoryFileExists(pipelineName, rev)) {
      return Collections.emptyList();
    }
    try {
      Reader reader = new FileReader(getPipelineStateHistoryFile(pipelineName, rev));
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
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteHistory(String pipelineName, String rev) {
    for (File f : getHistoryStateFiles(pipelineName, rev)) {
      f.delete();
    }
    LogUtil.resetRollingFileAppender(pipelineName, rev, STATE, getPipelineStateHistoryFile(pipelineName, rev)
      .getAbsolutePath(), configuration);
  }

  private void register(String pipelineName, String rev) {
    LogUtil.registerLogger(pipelineName, rev, STATE, getPipelineStateHistoryFile(pipelineName, rev).getAbsolutePath(),
      configuration);
  }

  private void persistPipelineState(PipelineState pipelineState) throws PipelineStoreException {
    // write to /runInfo/<pipelineName>/pipelineState.json as well as /runInfo/<pipelineName>/pipelineStateHistory.json
    try {
      ObjectMapperFactory.get().writeValue(
        (new DataStore(getPipelineStateFile(pipelineState.getName(), pipelineState.getRev())).getOutputStream()),
        BeanHelper.wrapPipelineState(pipelineState));

      // In addition, append the state of the pipeline to the pipelineState.json present in the directory of that
      // pipeline
      LogUtil.log(pipelineState.getName(), pipelineState.getRev(), STATE,
        ObjectMapperFactory.get().writeValueAsString(BeanHelper.wrapPipelineState(pipelineState)));

    } catch (IOException e) {
      LOG.error(ContainerError.CONTAINER_0100.getMessage(), e.getMessage(), e);
      throw new PipelineStoreException(ContainerError.CONTAINER_0100, e.getMessage(), e);
    }
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
