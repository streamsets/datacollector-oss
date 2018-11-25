/*
 * Copyright 2018 StreamSets Inc.
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

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

public class TestColonCompatibleChecker {

  @Test
  public void testStopAndDeleteCompatibility() throws Exception {
    RemoteDataCollector rc = Mockito.mock(RemoteDataCollector.class);
    ColonCompatibleRemoteDataCollector dc = Mockito.spy(new ColonCompatibleRemoteDataCollector(rc));
    Mockito.doReturn("name__foo").when(dc).getCompatibleName("name__foo");
    dc.stopAndDelete("user", "name__foo", "rev", 10L);
    Mockito.verify(rc, Mockito.times(1)).stopAndDelete(
        Mockito.eq("user"),
        Mockito.eq("name__foo"),
        Mockito.eq("rev"),
        Mockito.eq(10L)
    );
    Mockito.doReturn("name:foo").when(dc).getCompatibleName("name__foo");
    dc.stopAndDelete("user", "name__foo", "rev", 10L);
    Mockito.verify(rc, Mockito.times(1)).stopAndDelete(
        Mockito.eq("user"),
        Mockito.eq("name:foo"),
        Mockito.eq("rev"),
        Mockito.eq(10L)
    );
  }

  @Test
  public void testColonCompatibleName() throws Exception {
    RemoteDataCollector rc = Mockito.mock(RemoteDataCollector.class);
    PipelineStoreTask pipelineStoreTask = Mockito.mock(PipelineStoreTask.class);
    Mockito.doReturn(pipelineStoreTask).when(rc).getPipelineStoreTask();
    Mockito.doReturn(true).when(pipelineStoreTask).hasPipeline("name__foo");
    ColonCompatibleRemoteDataCollector dc = new ColonCompatibleRemoteDataCollector(rc);
    Assert.assertEquals("name__foo", dc.getCompatibleName("name__foo"));

    Mockito.doReturn(false).when(pipelineStoreTask).hasPipeline("name__foo");
    Mockito.doReturn(true).when(pipelineStoreTask).hasPipeline("name:foo");
    dc = new ColonCompatibleRemoteDataCollector(rc);
    Assert.assertEquals("name:foo", dc.getCompatibleName("name__foo"));
  }

}
