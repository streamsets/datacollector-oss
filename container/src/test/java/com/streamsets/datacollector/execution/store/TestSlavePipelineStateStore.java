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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;

import org.junit.Test;

import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStore;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.pipeline.api.ExecutionMode;

public class TestSlavePipelineStateStore {
  @Test
  public void testSaveState() throws Exception {
    SlavePipelineStateStore slavePipelineStateStore = new SlavePipelineStateStore();
    slavePipelineStateStore.saveState("user", "pipe1", "1", PipelineStatus.EDITED, "msg", new HashMap<String, Object>(),
      ExecutionMode.STANDALONE, null, 0, 0);
    assertEquals(PipelineStatus.EDITED, slavePipelineStateStore.getState("pipe1", "1").getStatus());
    try {
      slavePipelineStateStore.getState("blah", "blah");
      fail("Expected exception but didn't get any");
    } catch (PipelineStoreException ex) {
      // expected
    }

    try {
      slavePipelineStateStore.saveState("user", "pipe2", "1", PipelineStatus.EDITED, "msg", new HashMap<String, Object>(),
        ExecutionMode.STANDALONE, null, 0, 0);
      fail("Expected exception but didn't get any");
    } catch (PipelineStoreException ex) {
      // expected
    }
    try {
      slavePipelineStateStore.saveState("user", "pipe1", "2", PipelineStatus.EDITED, "msg", new HashMap<String, Object>(),
        ExecutionMode.STANDALONE, null, 0, 0);
      fail("Expected exception but didn't get any");
    } catch (PipelineStoreException ex) {
      // expected
    }
  }
}
