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
package com.streamsets.datacollector.execution.runner.cluster;

import com.google.common.io.Files;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.cluster.ApplicationState;
import com.streamsets.datacollector.cluster.MockClusterProvider;
import com.streamsets.datacollector.cluster.MockSystemProcess;
import com.streamsets.datacollector.cluster.MockSystemProcessFactory;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.StartPipelineContextBuilder;
import com.streamsets.datacollector.execution.cluster.ClusterHelper;
import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.execution.runner.cluster.ClusterRunner.ClusterSourceInfo;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.store.CachePipelineStateStore;
import com.streamsets.datacollector.execution.store.FilePipelineStateStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.streamsets.datacollector.util.AwaitConditionUtil.desiredPipelineState;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public class TestClusterRunner {

  private static final String SDC_TRANSIENT_ENV = RuntimeInfo.SDC_PRODUCT + RuntimeInfo.TRANSIENT_ENVIRONMENT_SUFFIX;
  private static final String APPID = "123";
  private static final String NAME = "p1";
  private static final String REV = "0";

  private static final ApplicationState APPLICATION_STATE = new ApplicationState();
  static {
    APPLICATION_STATE.setAppId(APPID);
  }
  private File tempDir;
  private File sparkManagerShell;
  private URLClassLoader emptyCL;
  private RuntimeInfo runtimeInfo;
  private BuildInfo buildInfo;
  private Configuration conf;
  private EventListenerManager eventListenerManager;
  private PipelineStoreTask pipelineStoreTask;
  private AclStoreTask aclStoreTask;
  private PipelineStateStore pipelineStateStore;
  private StageLibraryTask stageLibraryTask;
  private ClusterHelper clusterHelper;
  private MockClusterProvider clusterProvider;
  private Map<String, Object> attributes;
  private SafeScheduledExecutorService executorService;
  private StatsCollector statsCollector;

  @Before
  public void setup() throws Exception {
    executorService = new SafeScheduledExecutorService(2, "ClusterRunnerExecutor");
    emptyCL = new URLClassLoader(new URL[0]);
    tempDir = Files.createTempDir();
    Assert.assertTrue(tempDir.delete());
    Assert.assertTrue(tempDir.mkdir());
    System.setProperty(SDC_TRANSIENT_ENV, "true");
    System.setProperty("sdc.testing-mode", "true");
    File libexecDir = new File(tempDir, "libexec");
    Assert.assertTrue(libexecDir.mkdir());
    sparkManagerShell = new File(libexecDir, "_cluster-manager");
    Assert.assertTrue(sparkManagerShell.createNewFile());
    sparkManagerShell.setExecutable(true);
    MockSystemProcess.isAlive = false;
    MockSystemProcess.output.clear();
    MockSystemProcess.error.clear();
    runtimeInfo = new StandaloneRuntimeInfo("dummy", "dummy", null, Arrays.asList(emptyCL), tempDir);
    buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    clusterProvider = new MockClusterProvider();
    conf = new Configuration();
    pipelineStateStore = new CachePipelineStateStore(new FilePipelineStateStore(runtimeInfo, conf), conf);
    attributes = new HashMap<>();
    eventListenerManager = new EventListenerManager();
    stageLibraryTask = MockStages.createStageLibrary(emptyCL);
    pipelineStoreTask = new FilePipelineStoreTask(
        buildInfo,
        runtimeInfo,
        stageLibraryTask,
        pipelineStateStore,
        eventListenerManager,
        new LockCache<String>(),
        Mockito.mock(PipelineCredentialHandler.class),
        conf
    );
    pipelineStoreTask.init();
    pipelineStoreTask.create("admin", NAME, "label","some desc", false, false, attributes);
   //Create an invalid pipeline
    PipelineConfiguration pipelineConfiguration = pipelineStoreTask.create("user2", TestUtil.HIGHER_VERSION_PIPELINE,
        "label","description2", false, false, attributes
    );
    PipelineConfiguration mockPipelineConf = MockStages.createPipelineConfigurationSourceProcessorTargetHigherVersion();
    mockPipelineConf.getConfiguration().add(new Config("executionMode",
      ExecutionMode.CLUSTER_BATCH.name()));
    mockPipelineConf.getConfiguration().add(new Config("shouldRetry", "true"));
    mockPipelineConf.getConfiguration().add(new Config("retryAttempts", "3"));
    mockPipelineConf.setUuid(pipelineConfiguration.getUuid());
    pipelineStoreTask.save(
        "user2",
        TestUtil.HIGHER_VERSION_PIPELINE,
        "0",
        "description",
        mockPipelineConf,
        false
    );

    clusterHelper = new ClusterHelper(runtimeInfo, new MockSystemProcessFactory(), clusterProvider, tempDir, emptyCL, emptyCL, emptyCL, null);

    setExecModeAndRetries(ExecutionMode.CLUSTER_BATCH);
    aclStoreTask = new FileAclStoreTask(runtimeInfo, pipelineStoreTask, new LockCache<String>(),
        Mockito.mock(UserGroupManager.class));

    statsCollector = Mockito.mock(StatsCollector.class);
  }

  @After
  public void tearDown() {
    System.clearProperty(SDC_TRANSIENT_ENV);
    System.clearProperty("sdc.testing-mode");
    clusterProvider.submitTimesOut = false;
    clusterProvider.isRunningCommandFails = false;
    clusterProvider.isRunningTimesOut = false;
    clusterProvider.isSucceeded = false;
    clusterProvider.isRunning = true;
    FileUtils.deleteQuietly(tempDir);
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  private static class RetryPipelineStateStore extends CachePipelineStateStore {
    long retrySaveStateTime;
    public RetryPipelineStateStore(PipelineStateStore pipelineStateStore, Configuration conf) {
      super(pipelineStateStore, conf);
    }
    @Override
    public PipelineState saveState(String user, String name, String rev, PipelineStatus status, String message,
                                   Map<String, Object> attributes, ExecutionMode executionMode, String metrics, int retryAttempt,
                                   long nextRetryTimeStamp) throws PipelineStoreException {
      retrySaveStateTime = System.nanoTime();
      return super.saveState(user, name, rev, status, message, attributes, executionMode, metrics, retryAttempt,
          nextRetryTimeStamp);
    }

  }

  private static class RetryRunner extends ClusterRunner {
    static long retryInvocation;
    public RetryRunner(
        String name,
        String rev,
        RuntimeInfo runtimeInfo,
        BuildInfo buildInfo,
        Configuration configuration,
        PipelineStoreTask pipelineStore,
        PipelineStateStore pipelineStateStore,
        StageLibraryTask stageLibrary,
        SafeScheduledExecutorService executorService,
        ClusterHelper clusterHelper,
        ResourceManager resourceManager,
        EventListenerManager eventListenerManager,
        String sdcToken,
        StatsCollector statsCollector
    ) {
      super(
          name,
          rev,
          runtimeInfo,
          buildInfo,
          configuration,
          pipelineStore,
          pipelineStateStore,
          stageLibrary,
          executorService,
          clusterHelper,
          resourceManager,
          eventListenerManager,
          sdcToken,
          new FileAclStoreTask(runtimeInfo, pipelineStore, new LockCache<String>(),
              Mockito.mock(UserGroupManager.class)),
          statsCollector
      );
    }

    @Override
    protected ScheduledFuture<Void> scheduleForRetries(ScheduledExecutorService runnerExecutor) throws
        PipelineStoreException {
      retryInvocation = System.nanoTime();
      return super.scheduleForRetries(runnerExecutor);
    }
  }

  private void setExecModeAndRetries(ExecutionMode mode) throws Exception {
    PipelineConfiguration pipelineConf = pipelineStoreTask.load(NAME, REV);
    PipelineConfiguration conf = MockStages.createPipelineConfigurationWithClusterOnlyStage(mode);
    conf.setUuid(pipelineConf.getUuid());
    pipelineStoreTask.save("admin", NAME, REV, "", conf, false);
  }

  @Test
  public void testTransitionValidation() throws Exception {
    ClusterRunner clusterRunner = createClusterRunner();
    setState(PipelineStatus.FINISHED);
    assertEquals(PipelineStatus.FINISHED, clusterRunner.getState().getStatus());
    // can't transition from inactive state
    try {
      clusterRunner.setStateTransition(
              "admin", PipelineStatus.RUN_ERROR, "a", attributes);
      fail("Expected exception due to invalid transition");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0102, e.getErrorCode());
    }
    assertEquals(PipelineStatus.FINISHED, clusterRunner.getState().getStatus());

    // verify all public methods that do transitions that should be validated, but allowed when made directly
    testTransitionHelper(clusterRunner, PipelineStatus.STARTING,
            () -> clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build()));
    testTransitionHelper(clusterRunner, PipelineStatus.RUNNING,
            () -> clusterRunner.start(new StartPipelineContextBuilder("admin").build()));
    testTransitionHelper(clusterRunner, PipelineStatus.STOPPING,
            () -> clusterRunner.prepareForStop("admin"));
    testTransitionHelper(clusterRunner, PipelineStatus.STOPPED,
            () -> clusterRunner.stop("admin"));
  }

  private void testTransitionHelper(ClusterRunner clusterRunner,
                                    PipelineStatus finalStatus,
                                    TestRunnable callThatFails) throws Exception {
    // can't transition from DISCONNECTED to anything but CONNECTING in public methods
    setState(PipelineStatus.DISCONNECTED);
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
    try {
      callThatFails.run();
      fail("Expected exception due to invalid transition");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0102, e.getErrorCode());
    }
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());

    // internal transitions should go through
    clusterRunner.setStateTransition("admin", finalStatus, "directSet", attributes);
    assertEquals(finalStatus, clusterRunner.getState().getStatus());
  }

  @FunctionalInterface
  interface TestRunnable {
    void run() throws Exception;
  }

  @Test
  public void testPipelineRetry() throws Exception {
    PipelineStateStore pipelineStateStore = new RetryPipelineStateStore(new CachePipelineStateStore(new
        FilePipelineStateStore(runtimeInfo,
        conf
    ), conf), conf);
    Runner clusterRunner = createRunnerForRetryTest(pipelineStateStore);
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.STARTING, clusterRunner.getState().getStatus());
    clusterRunner.start(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getState().getStatus());
    ((ClusterRunner)clusterRunner).setStateTransition("admin", PipelineStatus.RUN_ERROR, "a", attributes);
    assertEquals(PipelineStatus.RETRY, clusterRunner.getState().getStatus());
    long saveStateTime = ((RetryPipelineStateStore)pipelineStateStore).retrySaveStateTime;
    long retryInvocationTime = ((RetryRunner)clusterRunner).retryInvocation;
    Assert.assertTrue("Retry should be schedule after state is saved", retryInvocationTime > saveStateTime);
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING, null, attributes, ExecutionMode.CLUSTER_MESOS_STREAMING, null, 1, 0);
    ((ClusterRunner)clusterRunner).setStateTransition("admin", PipelineStatus.RUN_ERROR, "a", attributes);
    assertEquals(PipelineStatus.RETRY, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING, null, attributes, ExecutionMode.CLUSTER_MESOS_STREAMING, null, 2, 0);
    ((ClusterRunner)clusterRunner).setStateTransition("admin", PipelineStatus.RUN_ERROR, "a", attributes);
    assertEquals(PipelineStatus.RETRY, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING, null, attributes, ExecutionMode.CLUSTER_MESOS_STREAMING, null, 3, 0);
    ((ClusterRunner)clusterRunner).setStateTransition("admin", PipelineStatus.RUN_ERROR, "a", attributes);
    assertEquals(PipelineStatus.RUN_ERROR, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelinePrepareDataCollectorStart() throws Exception {
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.EDITED, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING, null, attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.STARTING, null, attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
    pipelineStateStore
      .saveState("admin", NAME, "0", PipelineStatus.CONNECTING, null, attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.STOPPING, null, attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.STOPPED, null, attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.STOPPED, clusterRunner.getState().getStatus());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING_ERROR, null, attributes,
      ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    try {
      clusterRunner.prepareForDataCollectorStart("admin");
      fail("Expected exception but didn't get any");
    } catch (IllegalStateException ex) {
      // expected
    }
  }

  @Test
  public void testMetricsInStore() throws Exception {
    eventListenerManager = new EventListenerManager();
    MyClusterRunner clusterRunner =
      new MyClusterRunner(NAME, "0", runtimeInfo, buildInfo, conf, pipelineStoreTask, pipelineStateStore,
        stageLibraryTask, executorService, clusterHelper, new ResourceManager(conf), eventListenerManager, statsCollector);
    assertEquals("My_dummy_metrics", clusterRunner.getMetrics().toString());
    assertNull(clusterRunner.getState().getMetrics());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.RUNNING, null, attributes, ExecutionMode.CLUSTER_BATCH,
      null, 0, 0);
    clusterRunner.prepareForDataCollectorStart("admin");
    assertEquals("\"My_dummy_metrics\"", clusterRunner.getState().getMetrics());
    pipelineStateStore.saveState("admin", NAME, "0", PipelineStatus.CONNECTING, null, attributes,
      ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    assertNull(clusterRunner.getState().getMetrics());
  }

  private void setState(PipelineStatus status) throws Exception {
    pipelineStateStore.saveState("admin", NAME, "0", status, null, attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
  }

  @Test
  public void testPipelineStatusRunError() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    clusterProvider.isRunning = false;
    setState(PipelineStatus.RUNNING);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart("admin");
    Assert.assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
    clusterRunner.onDataCollectorStart("admin");
    Assert.assertEquals(PipelineStatus.RUN_ERROR, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStatusRunning() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    clusterProvider.isRunning = true;
    setState(PipelineStatus.DISCONNECTED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.onDataCollectorStart("admin");
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStatusConnectError() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    clusterProvider.isRunningTimesOut = true;
    setState(PipelineStatus.DISCONNECTED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.onDataCollectorStart("admin");
    Assert.assertEquals(PipelineStatus.CONNECT_ERROR, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStatusFinished() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    clusterProvider.isSucceeded = true;
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart("admin");
    clusterRunner.onDataCollectorStart("admin");
    Assert.assertEquals(PipelineStatus.FINISHED, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStatusDisconnected() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    ClusterRunner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart("admin");
    clusterRunner.onDataCollectorStop("stop");
    Assert.assertEquals(PipelineStatus.DISCONNECTED, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStatusStopped() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    ClusterRunner clusterRunner = Mockito.spy(createClusterRunner());
    Mockito.doReturn(Mockito.mock(PipelineConfiguration.class)).when(clusterRunner).getPipelineConfiguration("admin");
    clusterRunner.loadStartPipelineContextFromState("admin");
    clusterRunner.prepareForStop("admin");
    clusterRunner.stop("admin");
    Assert.assertEquals(PipelineStatus.STOPPED, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStatusStoppedConnectError() throws Exception {
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    setState(PipelineStatus.RUNNING);
    ClusterRunner clusterRunner = Mockito.spy(createClusterRunner());
    clusterRunner.loadStartPipelineContextFromState("admin");
    clusterProvider.killTimesOut = true;
    clusterRunner.prepareForStop("admin");
    clusterRunner.stop("admin");
    Assert.assertEquals(PipelineStatus.CONNECT_ERROR, clusterRunner.getState().getStatus());
    clusterProvider.killTimesOut = false;
    clusterRunner.prepareForStop("admin");
    clusterRunner.stop("admin");
    Assert.assertEquals(PipelineStatus.STOPPED, clusterRunner.getState().getStatus());
  }

  @Test
  public void testPipelineStartMultipleTimes() throws Exception {
    setState(PipelineStatus.EDITED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    clusterRunner.start(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getState().getStatus());

    // call start on the already running pipeline and make sure it doesn't request new resource each time
    for (int counter =0; counter < 10; counter++) {
      try {
        clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
        Assert.fail("Expected exception but didn't get any");
      } catch (PipelineRunnerException ex) {
        Assert.assertTrue(ex.getMessage().contains("CONTAINER_0102"));
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPipelineStatusStartError() throws Exception {
    setState(PipelineStatus.EDITED);
    Runner clusterRunner = createClusterRunner();
    clusterProvider.submitTimesOut = true;
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    clusterRunner.start(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.START_ERROR, clusterRunner.getState().getStatus());
    Mockito.verify(statsCollector).pipelineStatusChanged(
        Mockito.eq(PipelineStatus.START_ERROR), Mockito.any(), Mockito.isNull(Pipeline.class));
    clusterProvider.submitTimesOut = false;
    clusterProvider.appId = APPID;
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    clusterRunner.start(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getState().getStatus());
    Mockito.verify(statsCollector).pipelineStatusChanged(
        Mockito.eq(PipelineStatus.RUNNING), Mockito.any(), Mockito.isNull(Pipeline.class));
    ApplicationState appState = new ApplicationState((Map)pipelineStateStore.getState(NAME, REV).getAttributes().
      get(ClusterRunner.APPLICATION_STATE));
    assertEquals(APPID, appState.getAppId());
    clusterRunner.prepareForStop("admin");
    clusterRunner.stop("admin");
    assertEquals(PipelineStatus.STOPPED, clusterRunner.getState().getStatus());
    Mockito.verify(statsCollector).pipelineStatusChanged(
        Mockito.eq(PipelineStatus.STOPPED), Mockito.any(), Mockito.isNull(Pipeline.class));
    appState = new ApplicationState((Map)pipelineStateStore.getState(NAME, REV).getAttributes().
      get(ClusterRunner.APPLICATION_STATE));
    assertNull(appState.getAppId());
  }

  @Test
  public void testPipelineStatusRunningOnDataCollectorStart() throws Exception {
    setState(PipelineStatus.STARTING);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForDataCollectorStart("admin");
    clusterProvider.submitTimesOut = true;
    clusterRunner.onDataCollectorStart("admin");
    Assert.assertEquals(PipelineStatus.START_ERROR, clusterRunner.getState().getStatus());
  }


  @Test
  public void testSlaveList() throws Exception {
    ClusterRunner clusterRunner = (ClusterRunner) createClusterRunner();
    CallbackInfo callbackInfo = new CallbackInfo("user", "name", "rev", "myToken", "slaveToken", "",
      "", "", "", "", CallbackObjectType.METRICS, "","sdc_id");
    clusterRunner.updateSlaveCallbackInfo(callbackInfo);
    List<CallbackInfo> slaves = new ArrayList<CallbackInfo>(clusterRunner.getSlaveCallbackList(CallbackObjectType.METRICS));
    assertFalse(slaves.isEmpty());
    assertEquals("slaveToken", slaves.get(0).getSdcSlaveToken());
    assertEquals("myToken", slaves.get(0).getSdcClusterToken());
    assertEquals("sdc_id", slaves.get(0).getSlaveSdcId());
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    clusterRunner.start(new StartPipelineContextBuilder("admin").build());
    slaves = new ArrayList<>(clusterRunner.getSlaveCallbackList(CallbackObjectType.METRICS));
    assertTrue(slaves.isEmpty());
  }


  @Test
  public void testSlaveErrorCallbackList() throws Exception {
    ClusterRunner clusterRunner = Mockito.spy((ClusterRunner) createClusterRunner());
    String sampleError = "java.lang.NullPointerException\n" +
        "\tat com.streamsets.datacollector.execution.runner.cluster.TestClusterRunner.testSlaveErrorCallbackList(TestClusterRunner.java:416)";
    CallbackInfo callbackInfo = new CallbackInfo("user", "name", "rev", "myToken", "slaveToken", "",
        "", "", "", "", CallbackObjectType.ERROR, sampleError,"sdc_id");
    clusterRunner.updateSlaveCallbackInfo(callbackInfo);
    List<CallbackInfo> errorCallbacks = new ArrayList<>(clusterRunner.getSlaveCallbackList(CallbackObjectType.ERROR));
    assertFalse(errorCallbacks.isEmpty());
    assertEquals(1, errorCallbacks.size());

    clusterRunner.setStateTransition("user", PipelineStatus.STARTING, "Starting", Collections.<String, Object>emptyMap());
    clusterRunner.setStateTransition("user", PipelineStatus.RUNNING, "Running", Collections.<String, Object>emptyMap());

    Mockito.verify(clusterRunner, Mockito.never()).handleErrorCallbackFromSlaves(Matchers.anyMap());
    assertFalse(errorCallbacks.isEmpty());
    assertEquals(1, errorCallbacks.size());

    clusterRunner.setStateTransition("user", PipelineStatus.RUN_ERROR, "Run Error", new HashMap<String, Object>());
    //Check the handleErrorCallbackFromSlaves is called.
    Mockito.verify(clusterRunner, Mockito.times(1)).handleErrorCallbackFromSlaves(Matchers.anyMap());

    errorCallbacks = new ArrayList<>(clusterRunner.getSlaveCallbackList(CallbackObjectType.ERROR));
    assertEquals(0, errorCallbacks.size());
  }


  @Test
  public void testMultipleSlavesWithSameErrorCallbackObject() throws Exception {
    ClusterRunner clusterRunner = Mockito.spy((ClusterRunner) createClusterRunner());
    final String sampleError = "java.lang.NullPointerException\n" +
        "\tat com.streamsets.datacollector.execution.runner.cluster.TestClusterRunner.testSlaveErrorCallbackList(TestClusterRunner.java:416)";

    CallbackInfo callbackInfo1 = new CallbackInfo("user", "name", "rev", "myToken", "slaveToken", "url1",
        "", "", "", "", CallbackObjectType.ERROR, sampleError,"sdc_id1");
    CallbackInfo callbackInfo2 = new CallbackInfo("user", "name", "rev", "myToken", "slaveToken", "url2",
        "", "", "", "", CallbackObjectType.ERROR, sampleError,"sdc_id2");


    clusterRunner.updateSlaveCallbackInfo(callbackInfo1);
    clusterRunner.updateSlaveCallbackInfo(callbackInfo2);


    List<CallbackInfo> errorCallbacks = new ArrayList<>(clusterRunner.getSlaveCallbackList(CallbackObjectType.ERROR));
    assertFalse(errorCallbacks.isEmpty());
    assertEquals(2, errorCallbacks.size());

    clusterRunner.setStateTransition("user", PipelineStatus.STARTING, "Starting", Collections.<String, Object>emptyMap());

    clusterRunner.setStateTransition("user", PipelineStatus.RUNNING, "Running", Collections.<String, Object>emptyMap());

    Mockito.verify(clusterRunner, Mockito.never()).handleErrorCallbackFromSlaves(Matchers.anyMap());
    assertFalse(errorCallbacks.isEmpty());
    assertEquals(2, errorCallbacks.size());

    final Map<String, Object> attributeMap = new HashMap<>();

    clusterRunner.setStateTransition("user", PipelineStatus.RUN_ERROR, "Run Error", attributeMap);

    assertFalse(attributeMap.isEmpty());
    assertTrue(attributeMap.containsKey(ClusterRunner.SLAVE_ERROR_ATTRIBUTE));
    assertEquals(1, ((Set<String>)attributeMap.get(ClusterRunner.SLAVE_ERROR_ATTRIBUTE)).size());

    //Check the handleErrorCallbackFromSlaves is called.
    Mockito.verify(clusterRunner, Mockito.times(1)).handleErrorCallbackFromSlaves(Matchers.anyMap());

    errorCallbacks = new ArrayList<>(clusterRunner.getSlaveCallbackList(CallbackObjectType.ERROR));
    assertEquals(0, errorCallbacks.size());
  }

  @Test
  public void testGetParallelism() throws PipelineException, StageException {
    ClusterRunner clusterRunner = (ClusterRunner) createClusterRunner();
    ClusterSourceInfo clusterSourceInfo =
      clusterRunner.getClusterSourceInfo(new StartPipelineContextBuilder("admin").build(), NAME, REV,
        MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER_BATCH) // creates ClusterMSource
                                                                                          // which
        // has parallelism 25
        );
    Assert.assertEquals(25, clusterSourceInfo.getParallelism());
  }

  @Test
  public void testPipelineWithValidationIssues() throws PipelineException, StageException {
    ClusterRunner clusterRunner = (ClusterRunner) createClusterRunner();
    pipelineStateStore.saveState("admin", NAME, REV, PipelineStatus.STARTING, null, attributes, ExecutionMode.CLUSTER_BATCH,
      null, 0, 0);
    try {
      MockStages.ClusterMSource.MOCK_VALIDATION_ISSUES = true;
      clusterRunner.getClusterSourceInfo(new StartPipelineContextBuilder("admin").build(), NAME, REV,
        MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER_BATCH));
      fail("Expected PipelineRuntimeException but didn't get any");
    } catch (PipelineRuntimeException pe) {
      assertEquals(ContainerError.CONTAINER_0800, pe.getErrorCode());
      assertEquals(PipelineStatus.START_ERROR, clusterRunner.getState().getStatus());
    } catch (Exception e) {
      fail("Expected exception but got " + e);
    } finally {
      MockStages.ClusterMSource.MOCK_VALIDATION_ISSUES = false;
    }
  }

  @Test(timeout = 20000)
  public void testLoadingUnsupportedPipeline() throws Exception {
    Runner runner = createClusterRunnerForUnsupportedPipeline();
    pipelineStateStore.saveState("admin", TestUtil.HIGHER_VERSION_PIPELINE, REV, PipelineStatus.EDITED, null, attributes, ExecutionMode.CLUSTER_BATCH,
      null, 0, 0);
    runner.start(new StartPipelineContextBuilder("admin").build());
    await().until(desiredPipelineState(runner, PipelineStatus.START_ERROR));
    PipelineState state = runner.getState();
    Assert.assertTrue(state.getStatus() == PipelineStatus.START_ERROR);
    Assert.assertTrue(state.getMessage().contains("CONTAINER_0158"));
  }

  @Test
  public void tesOnDataCollectorStartUnsupportedPipeline1() throws Exception {
    pipelineStateStore.saveState("admin", TestUtil.HIGHER_VERSION_PIPELINE, "0", PipelineStatus.STARTING, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    Runner clusterRunner = createClusterRunnerForUnsupportedPipeline();
    clusterRunner.prepareForDataCollectorStart("admin");
    clusterProvider.submitTimesOut = true;
    clusterRunner.onDataCollectorStart("admin");
    await().until(desiredPipelineState(clusterRunner, PipelineStatus.START_ERROR));
    PipelineState state = clusterRunner.getState();
    Assert.assertTrue(state.getStatus() == PipelineStatus.START_ERROR);
    Assert.assertTrue(state.getMessage().contains("CONTAINER_0158"));
  }

  @Test
  public void testPipelineStatusStart() throws Exception {
    setState(PipelineStatus.EDITED);
    Runner clusterRunner = createClusterRunner();
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.STARTING, clusterRunner.getState().getStatus());

    clusterRunner.start(new StartPipelineContextBuilder("admin").build());
    Assert.assertEquals(PipelineStatus.RUNNING, clusterRunner.getState().getStatus());
  }

  @Test
  public void tesOnDataCollectorStartUnsupportedPipeline2() throws Exception {
    pipelineStateStore.saveState("admin", TestUtil.HIGHER_VERSION_PIPELINE, "0", PipelineStatus.RUNNING, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    Runner clusterRunner = createClusterRunnerForUnsupportedPipeline();
    clusterRunner.prepareForDataCollectorStart("admin");
    clusterProvider.submitTimesOut = true;
    clusterRunner.onDataCollectorStart("admin");
    await().until(desiredPipelineState(clusterRunner, PipelineStatus.START_ERROR));
    PipelineState state = clusterRunner.getState();
    Assert.assertTrue(state.getStatus() == PipelineStatus.START_ERROR);
    Assert.assertTrue(state.getMessage().contains("CONTAINER_0158"));
  }

  @Test
  public void testConnectTerminatesNormally() throws Exception {
    // onDataCollectorStart is the easiest way to test connect
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    ClusterRunner clusterRunner = createClusterRunner();
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    setState(PipelineStatus.RUNNING);

    // pipeline should exit
    clusterProvider.isSucceeded = true;
    clusterRunner.connect("admin", APPLICATION_STATE, pipelineStoreTask.load(NAME, REV), null);
    Assert.assertEquals(PipelineStatus.FINISHED, clusterRunner.getState().getStatus());
  }

  @Test
  public void testConnectTerminatesForced() throws Exception {
    // onDataCollectorStart is the easiest way to test connect
    attributes.put(ClusterRunner.APPLICATION_STATE, APPLICATION_STATE.getMap());
    ClusterRunner clusterRunner = Mockito.spy(createClusterRunner());
    clusterRunner.prepareForStart(new StartPipelineContextBuilder("admin").build());
    setState(PipelineStatus.RETRY);

    // make sure we are exercising an invalid transition
    try {
      clusterRunner.checkStatusTransition(PipelineStatus.FINISHED);
      fail("expected exception");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0102, e.getErrorCode());
    }

    // pipeline should get marked as FINISHED since the job was successful.
    clusterProvider.isSucceeded = true;
    clusterRunner.connect("admin", APPLICATION_STATE, pipelineStoreTask.load(NAME, REV), null);
    Assert.assertEquals(PipelineStatus.FINISHED, clusterRunner.getState().getStatus());
    Mockito.verify(clusterRunner).setStateTransition(
            Mockito.eq("admin"), Mockito.eq(PipelineStatus.FINISHED), Mockito.anyString(), Mockito.anyMap()
    );
  }

  @Test
  public void testRunningMaxPipelines() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(ExecutorConstants.RUNNER_THREAD_POOL_SIZE_KEY, 1);
    ResourceManager resourceManager = new ResourceManager(configuration);
    eventListenerManager.addStateEventListener(resourceManager);

    PipelineStoreTask pipelineStoreTask = new FilePipelineStoreTask(
        buildInfo,
        runtimeInfo,
        stageLibraryTask,
        pipelineStateStore,
        eventListenerManager,
        new LockCache<String>(),
        Mockito.mock(PipelineCredentialHandler.class),
        configuration
    );
    pipelineStoreTask.init();
    pipelineStoreTask.create("admin", "a", "label", "some desc", false, false, attributes);
    pipelineStateStore.saveState("admin", "a", "0", PipelineStatus.EDITED, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    pipelineStoreTask.create("admin", "b", "label","some desc", false, false, attributes);
    pipelineStateStore.saveState("admin", "b", "0", PipelineStatus.EDITED, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    pipelineStoreTask.create("admin", "c", "label","some desc", false, false, attributes);
    pipelineStateStore.saveState("admin", "c", "0", PipelineStatus.EDITED, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    pipelineStoreTask.create("admin", "d", "label","some desc", false, false, attributes);
    pipelineStateStore.saveState("admin", "d", "0", PipelineStatus.EDITED, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    pipelineStoreTask.create("admin", "e", "label","some desc", false, false, attributes);
    pipelineStateStore.saveState("admin", "e", "0", PipelineStatus.EDITED, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    pipelineStoreTask.create("admin", "f", "label","some desc", false, false, attributes);
    pipelineStateStore.saveState("admin", "f", "0", PipelineStatus.EDITED, null,
      attributes, ExecutionMode.CLUSTER_BATCH, null, 0, 0);

    //Only one runner can start pipeline at the max since the runner thread pool size is 3
    Runner runner1 = createClusterRunner("a", pipelineStoreTask, resourceManager);
    runner1.prepareForStart(new StartPipelineContextBuilder("admin").build());

    Runner runner2 = createClusterRunner("b", pipelineStoreTask, resourceManager);
    runner2.prepareForStart(new StartPipelineContextBuilder("admin").build());

    Runner runner3 = createClusterRunner("c", pipelineStoreTask, resourceManager);
    runner3.prepareForStart(new StartPipelineContextBuilder("admin").build());

    Runner runner4 = createClusterRunner("d", pipelineStoreTask, resourceManager);
    runner4.prepareForStart(new StartPipelineContextBuilder("admin").build());

    Runner runner5 = createClusterRunner("e", pipelineStoreTask, resourceManager);
    runner5.prepareForStart(new StartPipelineContextBuilder("admin").build());

    Runner runner6 = createClusterRunner("f", pipelineStoreTask, resourceManager);

    try {
      runner6.prepareForStart(new StartPipelineContextBuilder("admin").build());
      Assert.fail("PipelineRunnerException expected as sdc is out of runner thread resources");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0166, e.getErrorCode());
    }

    try {
      runner5.start(new StartPipelineContextBuilder("admin").build());
      Assert.fail("Expected exception as pipeline is empty");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0158, e.getErrorCode());
    }

    runner6.prepareForStart(new StartPipelineContextBuilder("admin").build());

    try {
      runner5.prepareForStart(new StartPipelineContextBuilder("admin").build());
      Assert.fail("PipelineRunnerException expected as sdc is out of runner thread resources");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0166, e.getErrorCode());
    }
  }

  private ClusterRunner createClusterRunner() {
    return new ClusterRunner(NAME, "0", runtimeInfo, buildInfo, conf, pipelineStoreTask, pipelineStateStore,
      stageLibraryTask, executorService, clusterHelper, new ResourceManager(conf), eventListenerManager, "myToken",
      aclStoreTask, statsCollector);
  }

  private Runner createRunnerForRetryTest(PipelineStateStore pipelineStateStore) {
    pipelineStateStore.init();
    return new RetryRunner(NAME, "0", runtimeInfo, buildInfo, conf, pipelineStoreTask, pipelineStateStore,
        stageLibraryTask, executorService, clusterHelper, new ResourceManager(conf), eventListenerManager, "myToken",
        statsCollector);
  }


  private Runner createClusterRunner(String name, PipelineStoreTask pipelineStoreTask, ResourceManager resourceManager) {
    return new ClusterRunner(name, "0", runtimeInfo, buildInfo, conf, pipelineStoreTask, pipelineStateStore,
        stageLibraryTask, executorService, clusterHelper, resourceManager, eventListenerManager, "myToken", aclStoreTask,
        statsCollector);
  }

  private Runner createClusterRunnerForUnsupportedPipeline() {
    return new AsyncRunner(
        new ClusterRunner(
            TestUtil.HIGHER_VERSION_PIPELINE,
            "0",
            runtimeInfo,
            buildInfo,
            conf,
            pipelineStoreTask,
            pipelineStateStore,
            stageLibraryTask,
            executorService,
            clusterHelper,
            new ResourceManager(conf),
            eventListenerManager,
            "myToken",
            aclStoreTask,
            statsCollector
        ),
        new SafeScheduledExecutorService(1, "runner"),
        new SafeScheduledExecutorService(1, "runnerStop")
        );
  }

  static class MyClusterRunner extends ClusterRunner {

    private static final boolean METRICS_TEST = true;

    MyClusterRunner(String name, String rev, RuntimeInfo runtimeInfo, BuildInfo buildInfo, Configuration configuration,
      PipelineStoreTask pipelineStore, PipelineStateStore pipelineStateStore, StageLibraryTask stageLibrary,
      SafeScheduledExecutorService executorService, ClusterHelper clusterHelper, ResourceManager resourceManager, EventListenerManager
      eventListenerManager, StatsCollector statsCollector) {
      super(name, rev, runtimeInfo, buildInfo, configuration, pipelineStore, pipelineStateStore, stageLibrary, executorService,
          clusterHelper, resourceManager, eventListenerManager, "myToken",
          new FileAclStoreTask(runtimeInfo, pipelineStore, new LockCache<String>(),
              Mockito.mock(UserGroupManager.class)),
          statsCollector);
    }

    @Override
    public Object getMetrics() {
      String metrics = "My_dummy_metrics";
      if (METRICS_TEST) {
        Object obj;
        return obj = metrics;
      } else {
        return getMetrics();
      }
    }

  }

}
