/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.cluster.ApplicationState;
import com.streamsets.pipeline.cluster.MockSparkProvider;
import com.streamsets.pipeline.cluster.MockSystemProcess;
import com.streamsets.pipeline.cluster.MockSystemProcessFactory;
import com.streamsets.pipeline.cluster.SparkManager;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.LogUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestClusterPipelineManager {
  private static final String APPID = "123";
  private static final String NAME = "p1";
  private static final String REV = "1";
  private static final ApplicationState APPLICATION_STATE = new ApplicationState();
  static {
    APPLICATION_STATE.setId(APPID);
  }
  private static final Set<State> ALL_STATES = ImmutableSet.copyOf(State.values());
  private File tempDir;
  private File sparkManagerShell;
  private URLClassLoader emptyCL;
  private RuntimeInfo runtimeInfo;
  private StateTracker stateTracker;
  private Configuration conf;
  private PipelineStoreTask pipelineStoreTask;
  private StageLibraryTask stageLibraryTask;
  private SparkManager sparkManager;
  private MockSparkProvider sparkProvider;
  private ClusterPipelineManager clusterPipelineManager;
  private Map<String, Object> attributes;

  @Before
  public void setup() throws Exception {
    LogUtil.unregisterAllLoggers();
    emptyCL = new URLClassLoader(new URL[0]);
    tempDir = Files.createTempDir();
    System.setProperty(RuntimeInfo.TRANSIENT_ENVIRONMENT, "true");
    System.setProperty("sdc.testing-mode", "true");
    sparkManagerShell = new File(tempDir, "spark-manager");
    Assert.assertTrue(tempDir.delete());
    Assert.assertTrue(tempDir.mkdir());
    Assert.assertTrue(sparkManagerShell.createNewFile());
    sparkManagerShell.setExecutable(true);
    MockSystemProcess.isAlive = false;
    MockSystemProcess.output.clear();
    MockSystemProcess.error.clear();
    runtimeInfo = new RuntimeInfo("dummy", null, Arrays.asList(emptyCL), tempDir);
    sparkProvider = new MockSparkProvider();
    conf = new Configuration();
    stateTracker = new StateTracker(runtimeInfo, conf);
    stateTracker.init();
    attributes = new HashMap<>();
    pipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, conf);
    pipelineStoreTask.init();
    pipelineStoreTask.create(NAME, "some desc", "admin");
    stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    sparkManager = new SparkManager(new MockSystemProcessFactory(), sparkProvider, tempDir, sparkManagerShell,
      emptyCL, emptyCL, 1);
    setExecMode(ExecutionMode.CLUSTER);
  }

  @After
  public void tearDown() {
    FileUtils.deleteQuietly(tempDir);
  }

  private void setExecMode(ExecutionMode mode) throws Exception {
    PipelineConfiguration pipelineConf = pipelineStoreTask.load(NAME, REV).createWithNewConfig(
      PipelineDefConfigs.EXECUTION_MODE_CONFIG, new ConfigConfiguration(PipelineDefConfigs.EXECUTION_MODE_CONFIG,
        mode));
    pipelineStoreTask.save(NAME, "admin", REV, "", pipelineConf);

  }
  private PipelineState createPipelineState(State state) {
    return new PipelineState(NAME, REV, state, "some state", System.currentTimeMillis(), null, attributes);
  }

  private State getState() throws Exception {
    Utils.checkState(ThreadUtil.sleep(20), "sleep was interrupted");
    return Utils.checkNotNull(stateTracker.getState(), "state").getState();
  }
  private void setState(State state) throws Exception {
    stateTracker.setState(NAME, REV, state, "msg", null, attributes);
  }

  private ClusterPipelineManager createClusterPipelineManager() {
    return new ClusterPipelineManager(runtimeInfo, conf, pipelineStoreTask, stageLibraryTask,
      sparkManager, stateTracker);
  }

  @Test
  public void testInitPipelineStateNull() throws Exception {
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
  }

  @Test
  public void testInitPipelineStateStopped() throws Exception {
    setState(State.STOPPED);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    Assert.assertEquals(State.STOPPED, getState());
  }

  @Test
  public void testInitPipelineStateRunning() throws Exception {
    attributes.put(ClusterPipelineManager.APPLICATION_STATE, APPLICATION_STATE);
    sparkProvider.isRunning = true;
    setState(State.RUNNING);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    Assert.assertEquals(State.RUNNING, getState());
  }

  @Test
  public void testInitPipelineStateNotRunning() throws Exception {
    attributes.put(ClusterPipelineManager.APPLICATION_STATE, APPLICATION_STATE);
    sparkProvider.isRunning = false;
    setState(State.RUNNING);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    Assert.assertEquals(State.ERROR, getState());
  }

  @Test
  public void testInitPipelineStateRunningTimesOut() throws Exception {
    attributes.put(ClusterPipelineManager.APPLICATION_STATE, APPLICATION_STATE);
    sparkProvider.isRunningTimesOut = true;
    setState(State.RUNNING);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    Assert.assertEquals(State.ERROR, getState());
  }

  /**
   * Init only modifies state when start is start
   */
  @Test
  public void testInitPipelineStateOtherStates() throws Exception {
    Set<State> states = new HashSet<>(ALL_STATES);
    states.remove(State.RUNNING);
    for (State state : states) {
      setState(state);
      clusterPipelineManager = createClusterPipelineManager();
      clusterPipelineManager.initTask();
      Assert.assertEquals(state, getState());
    }
  }

  @Test(expected = PipelineManagerException.class)
  public void testStartPipelineStandaloneMode() throws Exception {
    setExecMode(ExecutionMode.STANDALONE);
    attributes.put(ClusterPipelineManager.APPLICATION_STATE, APPLICATION_STATE);
    setState(State.STOPPED);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    // default is standalone
    clusterPipelineManager.startPipeline(NAME, REV);
  }

  @Test
  public void testStartPipelineClusterMode() throws Exception {
    sparkProvider.appId = APPID;
    setState(State.STOPPED);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    clusterPipelineManager.startPipeline(NAME, REV);
    Assert.assertEquals(State.RUNNING, getState());
    ApplicationState appState = (ApplicationState)stateTracker.getState().getAttributes().
      get(ClusterPipelineManager.APPLICATION_STATE);
    Assert.assertEquals(APPID, appState.getId());
    clusterPipelineManager.stopPipeline(false);
    Assert.assertEquals(State.STOPPED, getState());
    clusterPipelineManager.stopPipeline(false);
    Assert.assertEquals(State.STOPPED, getState());
    setState(State.STOPPED);
  }

  @Test
  public void testStartStopPipelineClusterModeError() throws Exception {
    sparkProvider.submitTimesOut = true;
    setState(State.STOPPED);
    clusterPipelineManager = createClusterPipelineManager();
    clusterPipelineManager.initTask();
    PipelineConfiguration pipelineConf = pipelineStoreTask.load(NAME, REV).createWithNewConfig(
      PipelineDefConfigs.EXECUTION_MODE_CONFIG, new ConfigConfiguration(PipelineDefConfigs.EXECUTION_MODE_CONFIG,
        ExecutionMode.CLUSTER));
    pipelineConf = pipelineStoreTask.save(NAME, "admin", REV, "", pipelineConf);
    clusterPipelineManager.startPipeline(NAME, REV);
    Assert.assertEquals(State.ERROR, getState());
    ApplicationState appState = (ApplicationState)stateTracker.getState().getAttributes().
      get(ClusterPipelineManager.APPLICATION_STATE);
    Assert.assertNull(appState);
  }
}
