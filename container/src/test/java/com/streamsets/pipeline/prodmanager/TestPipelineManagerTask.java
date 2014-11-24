/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestPipelineManagerTask {

  private ProductionPipelineManagerTask manager = null;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty("pipeline.data.dir", "./target/var");
    File f = new File(System.getProperty("pipeline.data.dir"));
    FileUtils.deleteDirectory(f);
    TestUtil.captureMockStages();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove("pipeline.data.dir");
  }

  @Before()
  public void setUp() throws PipelineStoreException, IOException {
    File f = new File(System.getProperty("pipeline.data.dir"));
    FileUtils.deleteDirectory(f);
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.when(configuration.get("maxBatchSize", 10)).thenReturn(10);
    FilePipelineStoreTask filePipelineStoreTask = Mockito.mock(FilePipelineStoreTask.class);
    Mockito.when(filePipelineStoreTask.load("xyz", "1.0")).thenReturn(Mockito.mock(PipelineConfiguration.class));

    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);

    manager = new ProductionPipelineManagerTask(info, configuration
        , filePipelineStoreTask, stageLibraryTask);
    manager.init();
  }

  @After
  public void tearDown() {
    manager.stop();
  }

  @Test
  public void testGetAndSetPipelineState() throws PipelineManagerException {
    Assert.assertEquals(State.STOPPED, manager.getPipelineState().getState());
    manager.setState("xyz", "1.0", State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());
    Assert.assertEquals("Started Running", manager.getPipelineState().getMessage());

    manager.setState("xyz", "1.0", State.ERROR, "Error");
    Assert.assertEquals(State.ERROR, manager.getPipelineState().getState());
    Assert.assertEquals("Error", manager.getPipelineState().getMessage());
  }

  @Test(expected = PipelineManagerException.class)
  public void testSetOffsetWhenRunning() throws PipelineManagerException, StageException, PipelineRuntimeException, PipelineStoreException {
    manager.setState("xyz", "1.0", State.RUNNING, "Started Running");
    manager.setOffset("abc");
  }

  @Test
  public void testSnapshotStatus() {
    SnapshotStatus snapshotStatus = manager.getSnapshotStatus();
    Assert.assertEquals(false, snapshotStatus.isExists());
    Assert.assertEquals(false, snapshotStatus.isSnapshotInProgress());

  }

  @Test(expected = PipelineManagerException.class)
  public void testStartPipelineWhenRunning() throws PipelineManagerException, StageException, PipelineRuntimeException, PipelineStoreException {

    Assert.assertEquals(State.STOPPED, manager.getPipelineState().getState());
    manager.setState("xyz", "1.0", State.RUNNING, "Started Running");
    Assert.assertEquals(State.RUNNING, manager.getPipelineState().getState());

    manager.startPipeline("xyz", "1.0");
  }

  @Test(expected = PipelineManagerException.class)
  public void testCaptureSnapshot() throws PipelineManagerException {
    //cannot capture snapshot when pipeline is not running
    manager.captureSnapshot(10);
  }

  @Test(expected = PipelineManagerException.class)
  public void testCaptureSnapshotInvalidBatch() throws PipelineManagerException {
    //cannot capture snapshot with wrong batch size
    manager.setState("xyz", "1.0", State.RUNNING, "Started Running");
    manager.captureSnapshot(0);
  }

  @Test(expected = PipelineManagerException.class)
  public void testStopPipelineWhenNotRunning() throws PipelineManagerException, StageException, PipelineRuntimeException, PipelineStoreException {
    Assert.assertEquals(State.STOPPED, manager.getPipelineState().getState());
    manager.stopPipeline();
  }

}
