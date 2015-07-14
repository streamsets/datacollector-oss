/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.store;

import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.manager.PipelineStateImpl;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.ContainerError;

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
      throw new PipelineStoreException(ContainerError.CONTAINER_0212, name, rev, RuntimeInfo.ExecutionMode.SLAVE,
        pipelineState.getName(), pipelineState.getRev());
    }
    pipelineState =
      new PipelineStateImpl(user, name, rev, status, message, System.currentTimeMillis(), attributes, executionMode);
    return pipelineState;
  }

  @Override
  public PipelineState getState(String name, String rev) throws PipelineStoreException {
    if (pipelineState != null && pipelineState.getName().equals(name) && pipelineState.getRev().equals(rev)) {
      return pipelineState;
    } else {
      throw new PipelineStoreException(ContainerError.CONTAINER_0211, name, rev, RuntimeInfo.ExecutionMode.SLAVE);
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
