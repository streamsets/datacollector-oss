/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public class TestPipelineManagerTask {

  /*private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "1.0";

  private ProductionPipelineManagerTask manager = null;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    TestUtil.captureMockStages();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before()
  public void setUp() throws PipelineStoreException, IOException, PipelineManagerException, StageException,
    PipelineRuntimeException {
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    Configuration configuration = Mockito.mock(Configuration.class);
    Mockito.when(configuration.get("maxBatchSize", 10)).thenReturn(10);
    FilePipelineStoreTask filePipelineStoreTask = Mockito.mock(FilePipelineStoreTask.class);
    Mockito.when(filePipelineStoreTask.load(PIPELINE_NAME, PIPELINE_REV)).thenReturn(Mockito.mock(PipelineConfiguration.class));

    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);

    manager = new ProductionPipelineManagerTask(info, configuration
        , filePipelineStoreTask, stageLibraryTask);
    manager.init();
  }

  @After
  public void tearDown() {
    manager.stop();
    //This may throw NullPointerException for some tests. This happens because we manually set the state as RUNNING
    //without actually running the pipeline which results in some internal variables not initialized. And then the
    //stop pipeline api tries to access them.
  }

  @Test
  public void testGetAndSetPipelineState() throws PipelineManagerException {
    Assert.assertNull(manager.getPipelineState());
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
    manager.stopPipeline(false);
  }*/

}
