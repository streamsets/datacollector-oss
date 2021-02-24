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
package com.streamsets.datacollector.execution.runner;

import com.codahale.metrics.MetricRegistry;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StartPipelineContextBuilder;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.execution.manager.PreviewerProvider;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.common.AsyncRunner;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.dc.execution.manager.standalone.ResourceManager;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.apache.commons.io.FileUtils;
import org.awaitility.Duration;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

import static com.streamsets.datacollector.util.AwaitConditionUtil.desiredPipelineState;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStandaloneRunner {

  private static final String DATA_DIR_KEY = RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR;

  private File dataDir;
  private ObjectGraph objectGraph;
  private StandaloneAndClusterPipelineManager pipelineManager;
  private PipelineStateStore pipelineStateStore;
  private StatsCollector statsCollector;

  @Before
  public void setUp() throws IOException {
    dataDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    dataDir.mkdirs();
    Assert.assertTrue("Could not create: " + dataDir, dataDir.isDirectory());
    System.setProperty(DATA_DIR_KEY, dataDir.getAbsolutePath());
    TestUtil.captureStagesForProductionRun();
    TestUtil.EMPTY_OFFSET = false;
    RuntimeInfo info = new StandaloneRuntimeInfo(
        RuntimeInfo.SDC_PRODUCT,
        RuntimeModule.SDC_PROPERTY_PREFIX,
        new MetricRegistry(),
        Arrays.asList(getClass().getClassLoader())
    );
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(info, TestUtil.MY_PIPELINE, "0").toPath());
    OffsetFileUtil.saveOffsets(info, TestUtil.MY_PIPELINE, "0", Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "dummy"));
    objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    statsCollector = objectGraph.get(StatsCollector.class);
    pipelineManager.init();
    pipelineManager.run();
  }

  @After
  public void tearDown() throws Exception {
    TestUtil.EMPTY_OFFSET = false;
    try {
      if (pipelineManager != null) {
        pipelineManager.stop();
      }
    } finally {
      TestUtil.EMPTY_OFFSET = false;
      System.getProperties().remove(DATA_DIR_KEY);
      await().atMost(Duration.ONE_MINUTE).until(() -> FileUtils.deleteQuietly(dataDir));
    }
  }

  @Test(timeout = 20000)
  public void testPipelineStart() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    Assert.assertNotNull(statsCollector);
    // Once from StandaloneRunner, another time from ProductionPipeline, since state is not centrally managed or reported
    Mockito.verify(statsCollector).pipelineStatusChanged(Matchers.eq(PipelineStatus.RUNNING), Matchers.any(), Matchers.any());
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
    Mockito.verify(statsCollector).pipelineStatusChanged(Matchers.eq(PipelineStatus.STOPPED), Matchers.any(), Matchers.any());
  }

  @Test(timeout = 20000)
  public void testPipelineStartWithParametersAndInterceptors() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0");

    Map<String, Object> runtimeParameters = new HashMap<>();
    runtimeParameters.put("param1", "Param1 Value");

    PipelineStartEvent.InterceptorConfiguration interceptorConfig = new PipelineStartEvent.InterceptorConfiguration();
    runner.start(new StartPipelineContextBuilder("admin")
      .withRuntimeParameters(runtimeParameters)
      .withInterceptorConfigurations(Collections.singletonList(interceptorConfig))
      .build()
    );
    waitForState(runner, PipelineStatus.RUNNING);

    PipelineState pipelineState = runner.getState();
    Map<String, Object> runtimeConstantsInState = (Map<String, Object>) pipelineState.getAttributes()
        .get(AbstractRunner.RUNTIME_PARAMETERS_ATTR);
    assertNotNull(runtimeConstantsInState);
    assertEquals(runtimeParameters.get("param1"), runtimeConstantsInState.get("param1"));
    assertFalse(pipelineState.getAttributes().containsKey("param1")); // Ensure that the parameters are not in the root of the attributes

    List<String> interceptorConfigsInState = (List<String>) pipelineState.getAttributes().get(AbstractRunner.INTERCEPTOR_CONFIGS_ATTR);
    assertNotNull(interceptorConfigsInState);
    assertEquals(1, interceptorConfigsInState.size());

    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
  }

  @Test(timeout = 20000)
  public void testPipelineLoadInterceptorsFromStateFile() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.MY_PIPELINE, "0");

    PipelineStartEvent.InterceptorConfiguration interceptorConfig = new PipelineStartEvent.InterceptorConfiguration();
    interceptorConfig.setStageLibrary("my-favourite");
    interceptorConfig.setInterceptorClassName("earth.us.sf.Klass");
    interceptorConfig.setParameters(Collections.singletonMap("enabled", "sure"));

    runner.start(new StartPipelineContextBuilder("admin")
      .withInterceptorConfigurations(Collections.singletonList(interceptorConfig))
      .build()
    );
    waitForState(runner, PipelineStatus.RUNNING);

    Runner.StartPipelineContext startContext = runner.getRunner(StandaloneRunner.class).loadStartPipelineContextFromState("user");
    assertNotNull(startContext);
    assertEquals(1, startContext.getInterceptorConfigurations().size());
    PipelineStartEvent.InterceptorConfiguration interceptorInState = startContext.getInterceptorConfigurations().get(0);
    assertNotNull(interceptorInState);
    assertEquals("my-favourite", interceptorInState.getStageLibrary());
    assertEquals("earth.us.sf.Klass", interceptorInState.getInterceptorClassName());
    assertEquals(1, interceptorInState.getParameters().size());
    assertEquals("sure", interceptorInState.getParameters().get("enabled"));

    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
  }

  @Test(timeout = 50000)
  public void testPipelinePrepare() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.FINISHING, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.FINISHED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STOPPING, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.STOPPED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.DISCONNECTING, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.CONNECTING, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STARTING, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.DISCONNECTED, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.prepareForDataCollectorStart("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STOPPED, "test save STOPPED", null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    assertEquals(PipelineStatus.STARTING, runner.getState().getStatus());
    /* TODO wait for RUNNING below should not be necessary. Without it, however, the following events occur:
     * Start thread                                     Stop thread (via async runner)
     * 1. ProductionPipeline transitions to STARTING
     *                                                  2. In main thread, transitions state to STOPPING.
     *
     * 3. PP tries to transition to RUNNING, fails
     * 4. PP checks PipelineRunner.stop, gets false.
     *    Has no handle to the current status (ie
     *    cannot observe that #2 occurred)
     * 5. PP transitions to RUN_ERROR (forced) when
     *    it is supposed to transition to STOPPED
     *                                                  6. Asynchronously, sets PipelineRunner.stop (too late!)
     *
     * We have a race condition between start and stop and no locking to protect it. Though PipelineRunner.stop is
     * volatile, it is set after the transition to STOPPING so it doesn't help in this race. If the stop thread
     * completes both steps atomically then there is never a problem, but these are two separate interface methods.
     */
    waitForState(runner, PipelineStatus.RUNNING);
    runner.stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STOPPED, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    assertNull(runner.getState().getMetrics());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RETRY, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner.start(new StartPipelineContextBuilder("admin").build());
    assertTrue("Unexpected state: " + runner.getState().getStatus(), runner.getState().getStatus().isOneOf(PipelineStatus.STARTING, PipelineStatus.RUNNING));
  }

  @Test
  public void testPipelineRetry() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    TestUtil.EMPTY_OFFSET = true;
    waitForState(runner, PipelineStatus.FINISHED);
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING_ERROR, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
    runner = runner.getRunner(AsyncRunner.class).getDelegatingRunner();
    ((StateListener)runner).stateChanged(PipelineStatus.RETRY, null, null);
    assertEquals(1, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getRetryAttempt());
    assertEquals(PipelineStatus.RETRY, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING_ERROR, null, null,
      ExecutionMode.STANDALONE, null, 1, 0);
    ((StateListener)runner).stateChanged(PipelineStatus.RETRY, null, null);
    assertEquals(2, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getRetryAttempt());
    assertEquals(PipelineStatus.RETRY, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING_ERROR, null, null,
      ExecutionMode.STANDALONE, null, 2, 0);
    ((StateListener)runner).stateChanged(PipelineStatus.RETRY, null, null);
    assertEquals(3, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getRetryAttempt());
    assertEquals(PipelineStatus.RETRY, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING_ERROR, null, null,
      ExecutionMode.STANDALONE, null, 3, 0);
    ((StateListener)runner).stateChanged(PipelineStatus.RETRY, null, null);
    assertEquals(0, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getRetryAttempt());
    assertEquals(PipelineStatus.RUN_ERROR, pipelineStateStore.getState(TestUtil.MY_PIPELINE, "0").getStatus());
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.STOPPED, null, null,
      ExecutionMode.STANDALONE, null, 0, 0);
  }

  @Test(timeout = 20000)
  public void testPipelineStartMultipleTimes() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    // call start on the already running pipeline and make sure it doesn't request new resource each time
    for (int counter =0; counter < 10; counter++) {
      try {
        runner.start(new StartPipelineContextBuilder("admin").build());
        Assert.fail("Expected exception but didn't get any");
      } catch (PipelineRunnerException ex) {
        Assert.assertTrue(ex.getMessage().contains("CONTAINER_0102"));
      }
    }
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
  }

  @Test(timeout = 20000)
  public void testPipelineFinish() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    assertNull(runner.getState().getMetrics());
    TestUtil.EMPTY_OFFSET = true;
    waitForState(runner, PipelineStatus.FINISHED);
    assertNotNull(runner.getState().getMetrics());
  }

  @Test(timeout = 20000)
  public void testLoadingUnsupportedPipeline() throws Exception {
    Runner runner = pipelineManager.getRunner(TestUtil.HIGHER_VERSION_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("user2").build());
    waitForState(runner, PipelineStatus.START_ERROR);
    PipelineState state = pipelineManager.getRunner(TestUtil.HIGHER_VERSION_PIPELINE, "0").getState();
    Assert.assertTrue(state.getStatus() == PipelineStatus.START_ERROR);
    Assert.assertTrue(state.getMessage().contains("CONTAINER_0158"));
    assertNull(runner.getState().getMetrics());
  }

  @Test(timeout = 20000)
  public void testDisconnectedPipelineStartedAgain() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    // sdc going down
    pipelineManager.stop();
    waitForState(runner, PipelineStatus.DISCONNECTED);

    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
    pipelineManager.run();

    runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    waitForState(runner, PipelineStatus.RUNNING);
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    Assert.assertTrue(runner.getState().getStatus() == PipelineStatus.STOPPED);
    assertNotNull(runner.getState().getMetrics());
  }

  @Test(timeout = 20000)
  public void testFinishedPipelineNotStartingAgain() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);

    TestUtil.EMPTY_OFFSET = true;
    waitForState(runner, PipelineStatus.FINISHED);

    // sdc going down
    pipelineManager.stop();
    assertNotNull(runner.getState().getMetrics());
    // Simulate finishing, the runner shouldn't restart on finishing
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.FINISHING, null, null,
      ExecutionMode.STANDALONE, runner.getState().getMetrics(), 0, 0);
    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
    pipelineManager.run();

    //Since SDC went down we need to get the runner again
    runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    waitForState(runner, PipelineStatus.FINISHED);
    assertNotNull(runner.getState().getMetrics());
  }

  @Test(timeout = 20000)
  public void testStopDcWhenPipelineInRetry() throws Exception {
    Runner runner = Mockito.spy(pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0"));
    ScheduledFuture<Void> retryFutureMock = Mockito.mock(ScheduledFuture.class);
    Whitebox.setInternalState(runner.getRunner(AsyncRunner.class).getDelegatingRunner(), "retryFuture", retryFutureMock);
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RETRY, "test set RETRY", null,
            ExecutionMode.STANDALONE, null, 0, 0);
    runner.onDataCollectorStop("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());

    // test forced transition
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RETRY, "test set RETRY", null,
            ExecutionMode.STANDALONE, null, 0, 0);
    // make it so when cancel is called, it simulates a concurrent thread transitioning to RUNNING_ERROR, which would
    // cause failures if not for forcing the transition
    Mockito.when(retryFutureMock.cancel(Mockito.anyBoolean())).thenAnswer(i -> {
      try {
        pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.RUNNING_ERROR,
                "test concurrent STARTING", null, ExecutionMode.STANDALONE, null, 0, 0);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return true;
    });
    runner.onDataCollectorStop("admin");
    assertEquals(PipelineStatus.DISCONNECTED, runner.getState().getStatus());
  }

  @Test(timeout = 20000)
  public void testMultiplePipelineStartStop() throws Exception {
    Runner runner1 = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    Runner runner2 = pipelineManager.getRunner( TestUtil.MY_SECOND_PIPELINE, "0");

    runner1.start(new StartPipelineContextBuilder("admin").build());
    runner2.start(new StartPipelineContextBuilder("admin2").build());
    waitForState(runner1, PipelineStatus.RUNNING);
    waitForState(runner2, PipelineStatus.RUNNING);

    runner1.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner1.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner1, PipelineStatus.STOPPED);
    runner2.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin2");
    runner2.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin2");
    Assert.assertTrue(runner2.getState().getStatus() == PipelineStatus.STOPPED);
    assertNotNull(runner1.getState().getMetrics());
    assertNotNull(runner2.getState().getMetrics());
  }

  @Test(timeout = 20000)
  public void testMultiplePipelineFinish() throws Exception {
    Runner runner1 = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    Runner runner2 = pipelineManager.getRunner( TestUtil.MY_SECOND_PIPELINE, "0");

    runner1.start(new StartPipelineContextBuilder("admin").build());
    runner2.start(new StartPipelineContextBuilder("admin2").build());
    waitForState(runner1, PipelineStatus.RUNNING);
    waitForState(runner2, PipelineStatus.RUNNING);

    TestUtil.EMPTY_OFFSET = true;

    waitForState(runner1, PipelineStatus.FINISHED);
    waitForState(runner2, PipelineStatus.FINISHED);
    assertNotNull(runner1.getState().getMetrics());
    assertNotNull(runner2.getState().getMetrics());
  }

  @Test(timeout = 20000)
  public void testDisconnectedPipelinesStartedAgain() throws Exception {
    Runner runner1 = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    Runner runner2 = pipelineManager.getRunner( TestUtil.MY_SECOND_PIPELINE, "0");
    runner1.start(new StartPipelineContextBuilder("admin").build());
    runner2.start(new StartPipelineContextBuilder("admin2").build());
    waitForState(runner1, PipelineStatus.RUNNING);
    waitForState(runner2, PipelineStatus.RUNNING);

    // sdc going down
    pipelineManager.stop();
    waitForState(runner1, PipelineStatus.DISCONNECTED);
    waitForState(runner2, PipelineStatus.DISCONNECTED);

    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule());
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
    pipelineManager.run();

    runner1 = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner2 = pipelineManager.getRunner( TestUtil.MY_SECOND_PIPELINE, "0");

    waitForState(runner1, PipelineStatus.RUNNING);
    waitForState(runner2, PipelineStatus.RUNNING);

    runner1.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner1.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    runner2.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin2");
    runner2.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin2");
    waitForState(runner1, PipelineStatus.STOPPED);
    waitForState(runner2, PipelineStatus.STOPPED);
    assertNotNull(runner1.getState().getMetrics());
    assertNotNull(runner2.getState().getMetrics());
  }

  private void waitForState(Runner runner, PipelineStatus pipelineStatus) throws Exception {
    try {
      await().until(desiredPipelineState(runner, pipelineStatus));
    } catch (ConditionTimeoutException e) {
      fail(String.format("Failed to transition to status %s, current status is %s",
              pipelineStatus, runner.getState().getStatus()));
    }
  }

  @Test(timeout = 20000)
  public void testPipelineStopTimeout() throws Exception {
    TestUtil.captureMockStagesLongWait();
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().forceQuit("admin");
    waitForState(runner, PipelineStatus.STOPPED);
  }

  @Test
  public void testSnapshot() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);

    //request to capture snapshot and check the status
    final String snapshotName = UUID.randomUUID().toString();
    String snapshotId = runner.captureSnapshot("admin", snapshotName, "snapshot label", 5, 10);
    assertNotNull(snapshotId);

    Snapshot snapshot = runner.getSnapshot(snapshotId);
    assertNotNull(snapshot);

    SnapshotInfo info = snapshot.getInfo();
    assertNotNull(info);
    assertNull(snapshot.getOutput());
    assertTrue(info.isInProgress());
    assertEquals(snapshotId, info.getId());
    assertEquals(TestUtil.MY_PIPELINE, info.getName());
    assertEquals("0", info.getRev());

    //update label
    snapshotId = runner.updateSnapshotLabel(snapshotName, "updated snapshot label");
    assertNotNull(snapshotId);
    snapshot = runner.getSnapshot(snapshotId);
    assertNotNull(snapshot);
    info = snapshot.getInfo();
    assertNotNull(info);
    assertEquals(snapshotName, info.getId());
    assertEquals("updated snapshot label", info.getLabel());

    //request cancel snapshot
    runner.deleteSnapshot(snapshotId);
    snapshot = runner.getSnapshot(snapshotId);
    assertNotNull(snapshot);
    assertNull(snapshot.getInfo());
    assertNull(snapshot.getOutput());

    //call cancel again - no op
    runner.deleteSnapshot(snapshotName);
    snapshot = runner.getSnapshot(snapshotId);
    assertNotNull(snapshot);
    assertNull(snapshot.getInfo());
    assertNull(snapshot.getOutput());

    //call cancel on some other snapshot which does not exist - no op
    runner.deleteSnapshot("mySnapshot1");
    snapshot = runner.getSnapshot(snapshotId);
    assertNotNull(snapshot);
    assertNull(snapshot.getInfo());
    assertNull(snapshot.getOutput());

    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
  }

  @Test (timeout = 60000)
  public void testStartAndCaptureSnapshot() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    final String snapshotId = UUID.randomUUID().toString();
    runner.startAndCaptureSnapshot(new StartPipelineContextBuilder("admin").build(), snapshotId, "snapshot label", 1, 10);
    waitForState(runner, PipelineStatus.RUNNING);

    await().until(() -> !runner.getSnapshot(snapshotId).getInfo().isInProgress());
    Snapshot snapshot = runner.getSnapshot(snapshotId);
    SnapshotInfo info = snapshot.getInfo();

    assertNotNull(snapshot.getOutput());
    assertFalse(info.isInProgress());
    assertEquals(snapshotId, info.getId());
    assertEquals(TestUtil.MY_PIPELINE, info.getName());
    assertEquals("0", info.getRev());
    assertEquals(1, info.getBatchNumber());

    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);

    // try with batch size less than 0
    String snapshotId1 = UUID.randomUUID().toString();
    try {
      runner.startAndCaptureSnapshot(new StartPipelineContextBuilder("admin").build(), snapshotId1, "snapshot label", 1, 0);
      Assert.fail("Expected PipelineRunnerException");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0107, e.getErrorCode());
    }
  }

  /**
   * SDC-6491: Make sure that snapshot will wait on first non-empty batch.
   */
  @Test (timeout = 60000)
  public void testEnsureNonEmptySnapshot() throws Exception {
    MockStages.setSourceCapture(new Source() {
      int counter = 0;

      // First 10 batches will "empty", then generate batches with exactly one record
      @Override
      public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        if(counter++ < 10) {
          return "skipping";
        }
        batchMaker.addRecord(new RecordImpl("stage", "id", null, null));
        return "generated-record";
      }

      @Override
      public List<ConfigIssue> init(Info info, Context context) {
        return Collections.emptyList();
      }

      @Override
      public void destroy() {
      }
    });

    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    final String snapshotId = UUID.randomUUID().toString();
    runner.startAndCaptureSnapshot(new StartPipelineContextBuilder("admin").build(), snapshotId, "snapshot label", 1, 10);
    waitForState(runner, PipelineStatus.RUNNING);

    await().until(() -> !runner.getSnapshot(snapshotId).getInfo().isInProgress());

    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);

    Snapshot snapshot = runner.getSnapshot(snapshotId);
    assertEquals(11, snapshot.getInfo().getBatchNumber());

    assertNotNull(snapshot.getOutput());
  }

  @Test (timeout = 60000)
  public void testRunningMaxPipelines() throws Exception {
    ObjectGraph objectGraph = ObjectGraph.create(new TestUtil.TestPipelineManagerModule(), ConfigModule.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
    pipelineManager.run();

    //Only one runner can start pipeline at the max since the runner thread pool size is 3
    Runner runner1 = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    runner1.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner1, PipelineStatus.RUNNING);

    Runner runner2 = pipelineManager.getRunner( TestUtil.MY_SECOND_PIPELINE, "0");
    try {
      runner2.start(new StartPipelineContextBuilder("admin2").build());
      Assert.fail("Expected PipelineRunnerException");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0166, e.getErrorCode());
    }

    runner1.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner1.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner1, PipelineStatus.STOPPED);

    runner2.start(new StartPipelineContextBuilder("admin2").build());
    waitForState(runner2, PipelineStatus.RUNNING);

    try {
      runner1.start(new StartPipelineContextBuilder("admin").build());
      Assert.fail("Expected PipelineRunnerException");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0166, e.getErrorCode());
    }

    runner2.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner2.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin2");
    waitForState(runner2, PipelineStatus.STOPPED);
  }

  @Test(timeout = 20000)
  public void testPipelineStoppedWithMail() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.PIPELINE_WITH_EMAIL, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().prepareForStop("admin");
    runner.getRunner(AsyncRunner.class).getDelegatingRunner().stop("admin");
    waitForState(runner, PipelineStatus.STOPPED);
    //wait for email
    GreenMail mailServer = TestUtil.TestRuntimeModule.getMailServer();
    while(mailServer.getReceivedMessages().length < 1) {
      ThreadUtil.sleep(100);
    }
    String headers = GreenMailUtil.getHeaders(mailServer.getReceivedMessages()[0]);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - " +
        TestUtil.PIPELINE_TITLE_WITH_EMAIL + " - STOPPED"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(mailServer.getReceivedMessages()[0]));

    mailServer.reset();
  }

  @Test(timeout = 20000)
  public void testPipelineFinishWithMail() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.PIPELINE_WITH_EMAIL, "0");
    runner.start(new StartPipelineContextBuilder("admin").build());
    waitForState(runner, PipelineStatus.RUNNING);
    assertNull(runner.getState().getMetrics());
    TestUtil.EMPTY_OFFSET = true;
    waitForState(runner, PipelineStatus.FINISHED);
    assertNotNull(runner.getState().getMetrics());
    //wait for email
    GreenMail mailServer = TestUtil.TestRuntimeModule.getMailServer();
    while(mailServer.getReceivedMessages().length < 1) {
      ThreadUtil.sleep(100);
    }
    String headers = GreenMailUtil.getHeaders(mailServer.getReceivedMessages()[0]);
    Assert.assertTrue(headers.contains("To: foo, bar"));
    Assert.assertTrue(headers.contains("Subject: StreamSets Data Collector Alert - " +
        TestUtil.PIPELINE_TITLE_WITH_EMAIL + " - FINISHED"));
    Assert.assertTrue(headers.contains("From: sdc@localhost"));
    Assert.assertNotNull(GreenMailUtil.getBody(mailServer.getReceivedMessages()[0]));

    mailServer.reset();
  }

  @Test(timeout = 20000)
  public void testMetricsEventRunnableCalledOnFinishPipeline() throws Exception {
    Runner runner = Mockito.spy(pipelineManager.getRunner( TestUtil.PIPELINE_WITH_EMAIL, "0"));
    StandaloneRunner standaloneRunner = Mockito.spy(runner.getRunner(StandaloneRunner.class));
    MetricsEventRunnable metricsEventRunnable =  Mockito.mock(MetricsEventRunnable.class);
    Mockito.doNothing().when(metricsEventRunnable).run();
    Whitebox.setInternalState(standaloneRunner, "metricsEventRunnable", metricsEventRunnable);
    standaloneRunner.stateChanged(PipelineStatus.STARTING, "", Collections.emptyMap());
    standaloneRunner.stateChanged(PipelineStatus.RUNNING, "", Collections.emptyMap());
    standaloneRunner.stateChanged(PipelineStatus.FINISHING, "", Collections.emptyMap());
    standaloneRunner.stateChanged(PipelineStatus.FINISHED, "", Collections.emptyMap());
    Mockito.verify(metricsEventRunnable, Mockito.times(1)).onStopOrFinishPipeline();
  }

  @Test
  public void testPreviewRegistersCorrectListener() throws Exception {
    // the previewer is mocked, so we can only test registration and invoking the listener method separately
    PreviewerProvider previewerProvider = objectGraph.get(PreviewerProvider.class);

    pipelineManager.createPreviewer(
        "user", TestUtil.MY_PIPELINE, "0", Collections.emptyList(), x -> null, false, new HashMap<>());

    Mockito.verify(previewerProvider).createPreviewer(
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.same(pipelineManager),
        Mockito.same(objectGraph),
        Mockito.anyListOf(PipelineStartEvent.InterceptorConfiguration.class),
        Mockito.any(),
        Mockito.anyBoolean(),
        Mockito.anyMap()
    );
  }

  @Test
  public void testPreviewStateChangeUpdatesStats() throws Exception {
    // the previewer is mocked, so we can only test registration and invoking the listener method separately
    Previewer previewer = pipelineManager.createPreviewer(
        "user", TestUtil.MY_PIPELINE, "0", Collections.emptyList(), x -> null, false, new HashMap<>());

    pipelineManager.statusChange(previewer.getId(), PreviewStatus.VALIDATING);
    Mockito.verify(statsCollector).previewStatusChanged(PreviewStatus.VALIDATING, previewer);
  }

  @Test
  public void testTransitionValidation() throws Exception {
    Runner runner = pipelineManager.getRunner( TestUtil.MY_PIPELINE, "0");
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.FINISHED, null, null,
            ExecutionMode.STANDALONE, null, 0, 0);
    StandaloneRunner standaloneRunner = runner.getRunner(StandaloneRunner.class);
    assertEquals(PipelineStatus.FINISHED, runner.getState().getStatus());
    // can't transition from inactive state
    try {
      standaloneRunner.stateChanged(PipelineStatus.RUN_ERROR, "failed direct transition test", null);
      fail("Expected exception due to invalid transition");
    } catch (PipelineRuntimeException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0102, e.getErrorCode());
    }
    assertEquals(PipelineStatus.FINISHED, standaloneRunner.getState().getStatus());

    // verify all public methods that do transitions that should be validated, but allowed when made directly
    testTransitionHelper(standaloneRunner, PipelineStatus.STARTING,
            () -> standaloneRunner.prepareForStart(new StartPipelineContextBuilder("admin").build()));
    testTransitionHelper(standaloneRunner, PipelineStatus.RUNNING,
            () -> standaloneRunner.start(new StartPipelineContextBuilder("admin").build()));
    testTransitionHelper(standaloneRunner, PipelineStatus.RUNNING,
            () -> standaloneRunner.startAndCaptureSnapshot(new StartPipelineContextBuilder("admin").build(),
                    null, null, 1, 1));
    testTransitionHelper(standaloneRunner, PipelineStatus.STOPPING,
            () -> standaloneRunner.prepareForStop("admin"));
    testTransitionHelper(standaloneRunner, PipelineStatus.STOPPED,
            () -> standaloneRunner.stop("admin"));
  }

  private void testTransitionHelper(StandaloneRunner standaloneRunner,
                                    PipelineStatus finalStatus,
                                    TestRunnable callThatFails) throws Exception {
    // can't transition from DISCONNECTING to anything but DISCONNECTED in public methods
    pipelineStateStore.saveState("admin", TestUtil.MY_PIPELINE, "0", PipelineStatus.DISCONNECTING, null, null,
            ExecutionMode.STANDALONE, null, 0, 0);
    assertEquals(PipelineStatus.DISCONNECTING, standaloneRunner.getState().getStatus());
    try {
      callThatFails.run();
      fail("Expected exception due to invalid transition");
    } catch (PipelineRunnerException e) {
      Assert.assertEquals(ContainerError.CONTAINER_0102, e.getErrorCode());
    }
    assertEquals(PipelineStatus.DISCONNECTING, standaloneRunner.getState().getStatus());

    // internal transitions should go through
    standaloneRunner.stateChanged(finalStatus, "successful direct transition", null);
    assertEquals(finalStatus, standaloneRunner.getState().getStatus());
  }

  @FunctionalInterface
  interface TestRunnable {
    void run() throws Exception;
  }

  @Module(overrides = true, library = true)
  static class ConfigModule {
    @Provides
    @Singleton
    public Configuration provideConfiguration() {
      Configuration configuration = new Configuration();
      configuration.set(ExecutorConstants.RUNNER_THREAD_POOL_SIZE_KEY, 3);
      return configuration;
    }

    @Provides
    @Singleton
    public ResourceManager provideResourceManager(Configuration configuration) {
      return new ResourceManager(configuration);
    }
  }

}
