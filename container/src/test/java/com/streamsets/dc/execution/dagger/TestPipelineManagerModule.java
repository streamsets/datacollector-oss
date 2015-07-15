/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.dagger;

import com.streamsets.dc.execution.Manager;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.manager.slave.SlavePipelineManager;
import com.streamsets.dc.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.dc.execution.runner.common.AsyncRunner;
import com.streamsets.dc.execution.runner.common.PipelineRunnerException;
import com.streamsets.dc.execution.runner.standalone.StandaloneRunner;
import com.streamsets.dc.main.MainSlavePipelineManagerModule;
import com.streamsets.dc.main.MainStandalonePipelineManagerModule;
import com.streamsets.dc.main.PipelineTask;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.SlavePipelineStoreTask;
import com.streamsets.pipeline.task.TaskWrapper;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestPipelineManagerModule {

  @Before
  public void setup() throws IOException {
    File testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, testDir.getAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Test
  public void testStandalonePipelineManagerModule() throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineRunnerException {
    //Start SDC and get an instance of PipelineTask
    ObjectGraph objectGraph = ObjectGraph.create(MainStandalonePipelineManagerModule.class);
    TaskWrapper taskWrapper = objectGraph.get(TaskWrapper.class);
    Assert.assertTrue(taskWrapper.getTask() instanceof PipelineTask);

    //Get an instance of manager
    taskWrapper.init();
    PipelineTask pipelineTask = (PipelineTask) taskWrapper.getTask();
    Manager pipelineManager = pipelineTask.getManager();
    Assert.assertTrue(pipelineManager instanceof StandaloneAndClusterPipelineManager);

    //Create previewer
    Previewer previewer = pipelineManager.createPreviewer("user", "p1", "1");
    assertEquals(previewer, pipelineManager.getPreview(previewer.getId()));
    ((StandaloneAndClusterPipelineManager)pipelineManager).outputRetrieved(previewer.getId());
    assertNull(pipelineManager.getPreview(previewer.getId()));

    //create and save empty pipeline
    PipelineStoreTask pipelineStoreTask = pipelineTask.getPipelineStoreTask();
    PipelineConfiguration pc = pipelineStoreTask.create("user", "p1", "description");
    pipelineStoreTask.save("user", "p1", "0", "description", pc);

    //create Runner
    Runner runner = pipelineManager.getRunner("user", "p1", "0");
    Assert.assertTrue(runner instanceof AsyncRunner);

    runner = ((AsyncRunner)runner).getRunner();
    Assert.assertTrue(runner instanceof StandaloneRunner);

    Assert.assertEquals(PipelineStatus.EDITED, runner.getStatus().getStatus());
    Assert.assertEquals("p1", runner.getName());
    Assert.assertEquals("0", runner.getRev());
  }

  @Test
  public void testSlavePipelineManagerModule() throws PipelineStoreException {
    ObjectGraph objectGraph = ObjectGraph.create(MainSlavePipelineManagerModule.class);
    TaskWrapper taskWrapper = objectGraph.get(TaskWrapper.class);
    taskWrapper.init();
    Assert.assertTrue(taskWrapper.getTask() instanceof PipelineTask);
    PipelineTask pipelineTask = (PipelineTask) taskWrapper.getTask();
    Manager pipelineManager = pipelineTask.getManager();
    Assert.assertTrue(pipelineManager instanceof SlavePipelineManager);

    try {
      pipelineManager.createPreviewer("user", "p1", "1");
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }

    PipelineStoreTask pipelineStoreTask = pipelineTask.getPipelineStoreTask();
    Assert.assertTrue(pipelineStoreTask instanceof SlavePipelineStoreTask);

    try {
      pipelineStoreTask.create("user", "p1", "description");
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }

    try {
      pipelineStoreTask.save("user", "p1", "0", "description", Mockito.mock(PipelineConfiguration.class));
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {

    }

    Runner runner = pipelineManager.getRunner("user", "p1", "0");
    Assert.assertTrue(runner instanceof AsyncRunner);

    AsyncRunner asyncRunner = (AsyncRunner)runner;

    Assert.assertEquals(PipelineStatus.EDITED, asyncRunner.getStatus().getStatus());
    Assert.assertEquals("p1", asyncRunner.getName());
    Assert.assertEquals("0", asyncRunner.getRev());
  }
}
