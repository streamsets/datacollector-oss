/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.runner.ClusterRunner.ClusterSourceInfo;
import com.streamsets.dataCollector.execution.store.CachePipelineStateStore;
import com.streamsets.dataCollector.execution.store.FilePipelineStateStore;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.cluster.ApplicationState;
import com.streamsets.dataCollector.execution.cluster.ClusterHelper;
import com.streamsets.pipeline.cluster.MockClusterProvider;
import com.streamsets.pipeline.cluster.MockSystemProcess;
import com.streamsets.pipeline.cluster.MockSystemProcessFactory;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;

public class TestClusterRunner {

  private static final String APPID = "123";
  private static final String NAME = "p1";
  private static final String REV = "0";
  private static final ApplicationState APPLICATION_STATE = new ApplicationState();
  static {
    APPLICATION_STATE.setId(APPID);
  }
  private File tempDir;
  private File sparkManagerShell;
  private URLClassLoader emptyCL;
  private RuntimeInfo runtimeInfo;
  private Configuration conf;
  private PipelineStoreTask pipelineStoreTask;
  private PipelineStateStore pipelineStateStore;
  private StageLibraryTask stageLibraryTask;
  private ClusterHelper clusterHelper;
  private MockClusterProvider clusterProvider;
  private Map<String, Object> attributes;
  private SafeScheduledExecutorService executorService;

  @Before
  public void setup() throws Exception {
    executorService = new SafeScheduledExecutorService(2, "ClusterRunnerExecutor");
    emptyCL = new URLClassLoader(new URL[0]);
    tempDir = Files.createTempDir();
    System.setProperty(RuntimeInfo.TRANSIENT_ENVIRONMENT, "true");
    System.setProperty("sdc.testing-mode", "true");
    sparkManagerShell = new File(tempDir, "_cluster-manager");
    Assert.assertTrue(tempDir.delete());
    Assert.assertTrue(tempDir.mkdir());
    Assert.assertTrue(sparkManagerShell.createNewFile());
    sparkManagerShell.setExecutable(true);
    MockSystemProcess.isAlive = false;
    MockSystemProcess.output.clear();
    MockSystemProcess.error.clear();
    runtimeInfo = new RuntimeInfo("dummy", null, Arrays.asList(emptyCL), tempDir);
    runtimeInfo.setSDCToken("myToken");
    clusterProvider = new MockClusterProvider();
    conf = new Configuration();
    pipelineStateStore = new CachePipelineStateStore(new FilePipelineStateStore(runtimeInfo, conf));
    attributes = new HashMap<>();
    stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    pipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, stageLibraryTask, pipelineStateStore);
    pipelineStoreTask.init();
    pipelineStoreTask.create("admin", NAME, "some desc");
    clusterHelper = new ClusterHelper(new MockSystemProcessFactory(), clusterProvider, tempDir, sparkManagerShell,
      emptyCL, emptyCL);
    setExecMode(ExecutionMode.CLUSTER);
  }

  @After
  public void tearDown() {
    System.clearProperty(RuntimeInfo.TRANSIENT_ENVIRONMENT);
    System.clearProperty("sdc.testing-mode");
    FileUtils.deleteQuietly(tempDir);
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  private void setExecMode(ExecutionMode mode) throws Exception {
    PipelineConfiguration pipelineConf = pipelineStoreTask.load(NAME, REV);
    PipelineConfiguration conf = MockStages.createPipelineConfigurationWithClusterOnlyStage(mode);
    conf.setUuid(pipelineConf.getUuid());
    pipelineStoreTask.save("admin", NAME, REV, "", conf);

  }

  @Test
  public void testPipelinePrepareDataCollectorStart() throws Exception {
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.EDITED, clusterRunner.getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING, null, attributes, ExecutionMode.CLUSTER);
    clusterRunner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.STARTING, null, attributes, ExecutionMode.CLUSTER);
    clusterRunner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getStatus());
    pipelineStateStore
      .saveState("admin", NAME, "0", PipelineStatus.CONNECTING, null, attributes, ExecutionMode.CLUSTER);
    clusterRunner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.STOPPING, null, attributes, ExecutionMode.CLUSTER);
    clusterRunner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.STOPPED, null, attributes, ExecutionMode.CLUSTER);
    clusterRunner.prepareForDataCollectorStart();
    assertEquals(PipelineStatus.STOPPED, clusterRunner.getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING_ERROR, null, attributes,
      ExecutionMode.CLUSTER);
    try {
      clusterRunner.prepareForDataCollectorStart();
      fail("Expected exception but didn't get any");
    } catch (IllegalStateException ex) {
      // expected
    }
  }

  private void setState(PipelineStatus status) throws Exception {
    pipelineStateStore.saveState("admin", NAME, "0", status, null, attributes, ExecutionMode.CLUSTER);
  }

  @Test
  public void testPipelineStatusRunError() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    clusterProvider.isRunning = false;
    setState(PipelineStatus.RUNNING);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart();
    Assert.assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getStatus());
    clusterRunner.onDataCollectorStart();
    Assert.assertEquals(PipelineStatus.RUN_ERROR, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusRunning() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    clusterProvider.isRunning = true;
    setState(PipelineStatus.DISCONNECTED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.onDataCollectorStart();
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusConnectError() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    clusterProvider.isRunningTimesOut = true;
    setState(PipelineStatus.DISCONNECTED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.onDataCollectorStart();
    Assert.assertEquals(PipelineStatus.CONNECT_ERROR, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusFinished() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    clusterProvider.isSucceeded = true;
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart();
    clusterRunner.onDataCollectorStart();
    Assert.assertEquals(PipelineStatus.FINISHED, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusDisconnected() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart();
    clusterRunner.onDataCollectorStop();
    Assert.assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusStopped() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.stop();
    Assert.assertEquals(PipelineStatus.STOPPED, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusStoppedConnectError() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    Runner clusterRunner = createClusterRunner();
    clusterProvider.killTimesOut = true;
    clusterRunner.stop();
    Assert.assertEquals(PipelineStatus.CONNECT_ERROR, clusterRunner.getStatus());
    clusterProvider.killTimesOut = false;
    clusterRunner.stop();
    Assert.assertEquals(PipelineStatus.STOPPED, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusStart() throws Exception {
    setState(PipelineStatus.EDITED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.start();
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getStatus());
  }

  @Test
  public void testPipelineStatusStartError() throws Exception {
    setState(PipelineStatus.EDITED);
    Runner clusterRunner = createClusterRunner();
    clusterProvider.submitTimesOut = true;
    clusterRunner.start();
    Assert.assertEquals(PipelineStatus.START_ERROR, clusterRunner.getStatus());
    clusterProvider.submitTimesOut = false;
    clusterProvider.appId = APPID;
    clusterRunner.start();
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getStatus());
    ApplicationState appState = new ApplicationState((Map)pipelineStateStore.getState(NAME, REV).getAttributes().
      get(ClusterRunner.APPLICATION_STATE));
    assertEquals(APPID, appState.getId());
    clusterRunner.stop();
    assertEquals(PipelineStatus.STOPPED, clusterRunner.getStatus());
    appState = new ApplicationState((Map)pipelineStateStore.getState(NAME, REV).getAttributes().
      get(ClusterRunner.APPLICATION_STATE));
    assertNull(appState.getId());
  }

  @Test
  public void testPipelineStatusRunningOnDataCollectorStart() throws Exception {
    setState(PipelineStatus.STARTING);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart();
    clusterProvider.submitTimesOut = true;
    clusterRunner.onDataCollectorStart();
    Assert.assertEquals(PipelineStatus.START_ERROR, clusterRunner.getStatus());
  }


  @Test
  public void testSlaveList() throws Exception {
    ClusterRunner clusterRunner = (ClusterRunner) createClusterRunner();
    CallbackInfo callbackInfo = new CallbackInfo(runtimeInfo.getSDCToken(), "slaveToken", "", "", "", "", "", "");
    clusterRunner.updateSlaveCallbackInfo(callbackInfo);
    List<CallbackInfo> slaves = new ArrayList<CallbackInfo>(clusterRunner.getSlaveCallbackList());
    assertFalse(slaves.isEmpty());
    assertEquals("slaveToken", slaves.get(0).getSdcSlaveToken());
    assertEquals(runtimeInfo.getSDCToken(), slaves.get(0).getSdcClusterToken());
    clusterRunner.start();
    slaves = new ArrayList<CallbackInfo>(clusterRunner.getSlaveCallbackList());
    assertTrue(slaves.isEmpty());
  }

  @Test
  public void testGetParallelism() throws PipelineRuntimeException, StageException, PipelineStoreException,
    PipelineManagerException {
    ClusterRunner clusterRunner = (ClusterRunner)createClusterRunner();
    ClusterSourceInfo clusterSourceInfo = clusterRunner.getClusterSourceInfo(NAME, REV,
      MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER) //creates ClusterMSource which
      //has parallelism 25
    );
    Assert.assertEquals(25, clusterSourceInfo.getParallelism());
    Assert.assertEquals("ClusterMSource", clusterSourceInfo.getClusterSourceName());
  }

  private Runner createClusterRunner() {
    return new ClusterRunner(NAME, "0", "admin", runtimeInfo, conf, pipelineStoreTask, pipelineStateStore,
      stageLibraryTask, executorService, clusterHelper);
  }


}
