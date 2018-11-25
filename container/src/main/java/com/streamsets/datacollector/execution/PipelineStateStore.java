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
package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.pipeline.api.ExecutionMode;

import java.util.List;
import java.util.Map;

// one per SDC
public interface PipelineStateStore {

  //PipelineStore should receive a PipelineStateStore instance and use it for 2 things
  // 1. when the pipeline is edited it should call the edited method to changed it state to EDITED
  // 2. the PipelineStore should reject any save/delete if the pipeline is running

  // the edited method should record only a change from <new> to EDITED or <other-status> to EDITED,
  // ignoring all EDITED to EDITED.
  public PipelineState edited(
      String user,
      String name,
      String rev,
      ExecutionMode executionMode,
      boolean isRemote,
      Map<String, Object> metadata
  ) throws PipelineStoreException;

 //called by PipelineStore when the pipeline is being deleted from the store.
  public void delete(String name, String rev) throws PipelineStoreException;

  public PipelineState saveState(
      String user,
      String name,
      String rev,
      PipelineStatus status,
      String message,
      Map<String, Object> attributes,
      ExecutionMode executionMode,
      String metrics,
      int retryAttempt,
      long nextRetryTimeStamp
  ) throws PipelineStoreException;

  public PipelineState getState(String name, String rev) throws PipelineStoreException;

  public List<PipelineState> getHistory(String name, String rev, boolean fromBeginning) throws PipelineStoreException;

  public void deleteHistory(String name, String rev);

  public void init();

  public void destroy();

}
