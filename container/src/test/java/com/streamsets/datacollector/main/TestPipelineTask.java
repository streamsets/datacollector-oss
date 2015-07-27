/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.main;


import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;

import org.junit.Test;
import org.mockito.Mockito;

public class TestPipelineTask {

  @Test
  public void testPipelineAgentDelegation() {
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    PipelineStoreTask store = Mockito.mock(PipelineStoreTask.class);
    WebServerTask webServer = Mockito.mock(WebServerTask.class);
    Manager pipelineManager = Mockito.mock(Manager.class);
    /*PipelineTask task = new PipelineTask(library, store, pipelineManager, webServer);
    Assert.assertEquals("pipelineNode", task.getName());*/
  }

}
