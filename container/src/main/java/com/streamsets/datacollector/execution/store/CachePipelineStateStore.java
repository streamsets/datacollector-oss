/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.store;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.ExecutionMode;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CachePipelineStateStore implements PipelineStateStore {
  private LoadingCache<String, PipelineState> pipelineStateCache;
  private final PipelineStateStore pipelineStateStore;
  private static final String NAME_AND_REV_SEPARATOR = "::";

  @Inject
  public CachePipelineStateStore(PipelineStateStore pipelineStateStore) {
   this.pipelineStateStore = pipelineStateStore;
  }

  @Override
  public void init() {
    pipelineStateCache = CacheBuilder.newBuilder().
      maximumSize(100).
      expireAfterAccess(10, TimeUnit.MINUTES).
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
  public PipelineState edited(String user, String name, String rev, ExecutionMode executionMode)
      throws PipelineStoreException {
    PipelineState pipelineState = pipelineStateStore.edited(user, name, rev, executionMode);
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
      Map<String, Object> attributes, ExecutionMode executionMode, String metrics) throws PipelineStoreException {
    PipelineState pipelineState = pipelineStateStore.saveState(user, name, rev, status, message, attributes,
                                                               executionMode, metrics);
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
        throw new PipelineStoreException(ContainerError.CONTAINER_0114, ex.getMessage(), ex);
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
