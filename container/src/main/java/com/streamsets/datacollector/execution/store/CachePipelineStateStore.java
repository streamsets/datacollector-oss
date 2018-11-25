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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.ExecutionMode;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CachePipelineStateStore implements PipelineStateStore {
  private static final String PIPELINE_STATE_CACHE_MAXIMUM_SIZE = "store.pipeline.state.cache.maximum.size";
  private static final int PIPELINE_STATE_CACHE_MAXIMUM_SIZE_DEFAULT = 100;
  private static final String PIPELINE_STATE_CACHE_EXPIRE_AFTER_ACCESS =
      "store.pipeline.state.cache.expire.after.access";
  private static final int PIPELINE_STATE_CACHE_EXPIRE_AFTER_ACCESS_DEFAULT = 10;


  private final Configuration configuration;
  private LoadingCache<String, PipelineState> pipelineStateCache;
  private final PipelineStateStore pipelineStateStore;
  private static final String NAME_AND_REV_SEPARATOR = "::";

  @Inject
  public CachePipelineStateStore(PipelineStateStore pipelineStateStore, Configuration conf) {
   this.pipelineStateStore = pipelineStateStore;
    this.configuration = conf;
  }

  @Override
  public void init() {
    pipelineStateCache = CacheBuilder.newBuilder().
      maximumSize(configuration.get(PIPELINE_STATE_CACHE_MAXIMUM_SIZE, PIPELINE_STATE_CACHE_MAXIMUM_SIZE_DEFAULT)).
      expireAfterAccess(
          configuration.get(PIPELINE_STATE_CACHE_EXPIRE_AFTER_ACCESS, PIPELINE_STATE_CACHE_EXPIRE_AFTER_ACCESS_DEFAULT),
          TimeUnit.MINUTES
      ).
      build(new CacheLoader<String, PipelineState>() {
      @Override
      public PipelineState load(String nameAndRev) throws Exception {
        return pipelineStateStore.getState(getName(nameAndRev), getRev(nameAndRev));
      }
    });
    pipelineStateStore.init();
  }

  @Override
  public void destroy() {
    pipelineStateCache.invalidateAll();
    pipelineStateStore.destroy();
  }

  @Override
  public PipelineState edited(
      String user, String name, String rev, ExecutionMode executionMode, boolean isRemote, Map<String, Object> metadata
  )
      throws PipelineStoreException {
    PipelineState pipelineState = pipelineStateStore.edited(user, name, rev, executionMode, isRemote, metadata);
    if (pipelineState != null) {
      pipelineStateCache.put(getNameAndRevString(name, rev), pipelineState);
    }
    return pipelineState;
  }

  @Override
  public void delete(String name, String rev) throws PipelineStoreException {
    pipelineStateStore.delete(name, rev);
    pipelineStateCache.invalidate(getNameAndRevString(name, rev));
  }

  @Override
  public PipelineState saveState(String user, String name, String rev, PipelineStatus status, String message,
      Map<String, Object> attributes, ExecutionMode executionMode, String metrics, int retryAttempt,
                                 long nextRetryTimeStamp) throws PipelineStoreException {
    PipelineState pipelineState = pipelineStateStore.saveState(
        user, name, rev, status, message, attributes, executionMode, metrics, retryAttempt, nextRetryTimeStamp);
    pipelineStateCache.put(getNameAndRevString(name, rev), pipelineState);
    return pipelineState;
  }

  @Override
  public PipelineState getState(String name, String rev) throws PipelineStoreException  {
    try {
      return pipelineStateCache.get(getNameAndRevString(name, rev));
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof RuntimeException) {
        throw (RuntimeException) ex.getCause();
      } else if (ex.getCause() instanceof PipelineStoreException) {
        throw (PipelineStoreException) ex.getCause();
      } else {
        throw new PipelineStoreException(ContainerError.CONTAINER_0114, ex.toString(), ex);
      }
    }
  }

  @Override
  public List<PipelineState> getHistory(String name, String rev, boolean fromBeginning) throws PipelineStoreException {
    // We dont cache history, get from persistent store directly
    return pipelineStateStore.getHistory(name, rev, fromBeginning);
  }

  @Override
  public void deleteHistory(String name, String rev) {
    pipelineStateStore.deleteHistory(name, rev);
  }

  private String getNameAndRevString(String name, String rev) {
    return name + NAME_AND_REV_SEPARATOR + rev;
  }

  private String getName(String nameAndRevString) {
    return nameAndRevString.split(NAME_AND_REV_SEPARATOR)[0];
  }

  private String getRev(String nameAndRevString) {
    return nameAndRevString.split(NAME_AND_REV_SEPARATOR)[1];
  }

}
