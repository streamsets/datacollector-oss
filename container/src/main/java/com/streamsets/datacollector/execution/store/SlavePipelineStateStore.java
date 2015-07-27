/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.store;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.ExecutionMode;

import java.util.List;
import java.util.Map;

public class SlavePipelineStateStore implements PipelineStateStore {

  // In slave we support only one pipeline
  private volatile PipelineState pipelineState;

  @Override
  public PipelineState edited(String user, String name, String rev, ExecutionMode executionMode)
    throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name, String rev) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineState saveState(String user, String name, String rev, PipelineStatus status, String message,
    Map<String, Object> attributes, ExecutionMode executionMode) throws PipelineStoreException {
    if (pipelineState != null && (!pipelineState.getName().equals(name) || !pipelineState.getRev().equals(rev))) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0212, name, rev, ExecutionMode.SLAVE,
        pipelineState.getName(), pipelineState.getRev());
    }
    pipelineState =
      new PipelineStateImpl(user, name, rev, status, message, System.currentTimeMillis(), attributes, ExecutionMode.SLAVE);
    return pipelineState;
  }

  @Override
  public PipelineState getState(String name, String rev) throws PipelineStoreException {
    if (pipelineState != null && pipelineState.getName().equals(name) && pipelineState.getRev().equals(rev)) {
      return pipelineState;
    } else {
      throw new PipelineStoreException(ContainerError.CONTAINER_0211, name, rev, ExecutionMode.SLAVE);
    }
  }

  @Override
  public List<PipelineState> getHistory(String name, String rev, boolean fromBeginning) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteHistory(String name, String rev) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init() {
    // Do nothing
  }

  @Override
  public void destroy() {
    // Do nothing
  }

}
