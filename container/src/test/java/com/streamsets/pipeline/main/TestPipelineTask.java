/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;


import com.streamsets.pipeline.callback.CallbackServerTask;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.prodmanager.StandalonePipelineManagerTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPipelineTask {

  @Test
  public void testPipelineAgentDelegation() {
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    PipelineStoreTask store = Mockito.mock(PipelineStoreTask.class);
    WebServerTask webServer = Mockito.mock(WebServerTask.class);
    StandalonePipelineManagerTask pipelineManager = Mockito.mock(StandalonePipelineManagerTask.class);
    CallbackServerTask callbackServerTask = Mockito.mock(CallbackServerTask.class);
    PipelineTask task = new PipelineTask(library, store, pipelineManager, webServer, callbackServerTask);
    Assert.assertEquals("pipelineNode", task.getName());
  }

}
