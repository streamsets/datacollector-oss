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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.ExecutionMode;

import java.util.List;
import java.util.Map;

public class SlavePipelineStateStore implements PipelineStateStore {

  // In slave we support only one pipeline
  private volatile PipelineState pipelineState;

  @Override
  public PipelineState edited(
      String user, String name, String rev, ExecutionMode executionMode, boolean isRemote, Map<String, Object> metadata
  )
    throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(String name, String rev) throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PipelineState saveState(String user, String name, String rev, PipelineStatus status, String message,
    Map<String, Object> attributes, ExecutionMode executionMode, String metrics, int retryAttempt, long nextRetryTimeStamp) throws PipelineStoreException {
    if (pipelineState != null && (!pipelineState.getPipelineId().equals(name) || !pipelineState.getRev().equals(rev))) {
      throw new PipelineStoreException(ContainerError.CONTAINER_0212, name, rev, ExecutionMode.SLAVE,
        pipelineState.getPipelineId(), pipelineState.getRev());
    }
    if (attributes == null) {
      attributes = getState(name, rev).getAttributes();
    }
    pipelineState =
      new PipelineStateImpl(user, name, rev, status, message, System.currentTimeMillis(), attributes,
        ExecutionMode.SLAVE, metrics, retryAttempt, nextRetryTimeStamp);
    return pipelineState;
  }

  @Override
  public PipelineState getState(String name, String rev) throws PipelineStoreException {
    if (pipelineState != null && pipelineState.getPipelineId().equals(name) && pipelineState.getRev().equals(rev)) {
      return pipelineState;
    } else {
      throw new PipelineStoreException(ContainerError.CONTAINER_0211, name, rev, ExecutionMode.SLAVE);
    }
  }

  @Override
  public List<PipelineState> getHistory(String name, String rev, boolean fromBeginning) throws PipelineStoreException {
    return ImmutableList.of(pipelineState);
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
