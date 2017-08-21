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
package com.streamsets.datacollector.event.handler.remote;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Source;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestRemoteStateEventListener {

  @Test
  public void testRemoteStateEventListener() throws Exception {
    RemoteStateEventListener remoteStateEventListener = new RemoteStateEventListener(new Configuration());
    remoteStateEventListener.init();
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(RemoteDataCollector.IS_REMOTE_PIPELINE, true);
    PipelineState pipelineState = new PipelineStateImpl("user", "name", "0", PipelineStatus.RUNNING, "msg", -1,
        attributes, ExecutionMode.STANDALONE, "", 1, -1);
    remoteStateEventListener.onStateChange(null, pipelineState, null, null, Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:1000"));
    Collection<Pair<PipelineState, Map<String, String>>> pipelineStateAndOffset = remoteStateEventListener.getPipelineStateEvents();
    Assert.assertEquals(1, pipelineStateAndOffset.size());
    Assert.assertEquals(PipelineStatus.RUNNING, pipelineStateAndOffset.iterator().next().getLeft().getStatus());
    Assert.assertEquals("offset:1000", pipelineStateAndOffset.iterator().next().getRight().get(Source.POLL_SOURCE_OFFSET_KEY));
    Assert.assertEquals(0, remoteStateEventListener.getPipelineStateEvents().size());

    PipelineState pipelineStateDeleted = new PipelineStateImpl("user", "name", "0", PipelineStatus.DELETED, "msg", -1,
        attributes, ExecutionMode.STANDALONE, "", 1, -1);
    remoteStateEventListener.onStateChange(null, pipelineState, null, null, Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:old"));
    remoteStateEventListener.onStateChange(null, pipelineStateDeleted, null, null, Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:new"));
    pipelineStateAndOffset = remoteStateEventListener.getPipelineStateEvents();
    Assert.assertEquals(2, pipelineStateAndOffset.size());
    Iterator<Pair<PipelineState, Map<String, String>>> iterator = pipelineStateAndOffset.iterator();
    iterator.next();
    Pair<PipelineState, Map<String, String>> pair = iterator.next();
    Assert.assertEquals(PipelineStatus.DELETED, pair.getLeft().getStatus());
    Assert.assertEquals("offset:new", pair.getRight().get(Source.POLL_SOURCE_OFFSET_KEY));
    Assert.assertEquals(0, remoteStateEventListener.getPipelineStateEvents().size());
  }

}
