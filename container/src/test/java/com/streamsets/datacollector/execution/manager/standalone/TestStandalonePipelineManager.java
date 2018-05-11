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
package com.streamsets.datacollector.execution.manager.standalone;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.PreviewerListener;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.execution.manager.PreviewerProvider;
import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.execution.runner.provider.StandaloneAndClusterRunnerProviderImpl;
import com.streamsets.datacollector.execution.runner.standalone.StandaloneRunner;
import com.streamsets.datacollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.datacollector.execution.store.FilePipelineStateStore;
import com.streamsets.datacollector.lineage.LineagePublisherTask;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.apache.commons.io.FileUtils;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.streamsets.datacollector.util.AwaitConditionUtil.numPipelinesEqualTo;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStandalonePipelineManager {

  private static Logger LOG = LoggerFactory.getLogger(TestStandalonePipelineManager.class);

  private PipelineStoreTask pipelineStoreTask;
  private Manager pipelineManager;
  private PipelineStateStore pipelineStateStore;

  @Module(
    injects = {
      StandaloneAndClusterPipelineManager.class,
      PipelineStoreTask.class,
      PipelineStateStore.class,
      StandaloneRunner.class,
      EventListenerManager.class,
      LockCache.class,
      RuntimeInfo.class
    },
    includes = LockCacheModule.class,
    library = true
  )
  public static class TestPipelineManagerModule {
    private static Logger LOG = LoggerFactory.getLogger(TestPipelineManagerModule.class);
    private final long expiry;
    private final long initialExpiryDelay;

    public TestPipelineManagerModule(long expiry, long initialExpiryDelay) {
      this.initialExpiryDelay = initialExpiryDelay;
      this.expiry = expiry;
    }

    @Provides @Singleton
    public BuildInfo provideBuildInfo() {
      return new DataCollectorBuildInfo();
    }

    @Provides @Singleton
    public RuntimeInfo providesRuntimeInfo() {
      RuntimeInfo runtimeInfo = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestStandalonePipelineManager.class.getClassLoader()));

      File targetDir = new File("target", UUID.randomUUID().toString());
      targetDir.mkdir();
      File absFile = new File(targetDir, "_cluster-manager");
      try {
        absFile.createNewFile();
      } catch (IOException e) {
        LOG.info("Got exception " + e, e);
      }
      System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LIBEXEC_DIR,
        targetDir.getAbsolutePath());
      absFile.setExecutable(true);
      return runtimeInfo;
    }

    @Provides @Singleton
    public Configuration provideConfiguration() {
      Configuration configuration = new Configuration();
      configuration.set(StandaloneAndClusterPipelineManager.RUNNER_EXPIRY_INTERVAL, expiry);
      configuration.set(StandaloneAndClusterPipelineManager.RUNNER_EXPIRY_INITIAL_DELAY, initialExpiryDelay);
      return configuration;
    }

    @Provides @Singleton
    public PipelineStoreTask providePipelineStoreTask(RuntimeInfo runtimeInfo, StageLibraryTask stageLibraryTask,
                                                      PipelineStateStore pipelineStateStore, LockCache<String> lockCache) {
      FilePipelineStoreTask filePipelineStoreTask = new FilePipelineStoreTask(runtimeInfo, stageLibraryTask,
        pipelineStateStore, lockCache);
      filePipelineStoreTask.init();
      return filePipelineStoreTask;
    }

    @Provides @Singleton
    public AclStoreTask provideAclStoreTask(
        RuntimeInfo runtimeInfo,
        PipelineStoreTask pipelineStoreTask,
        LockCache<String> lockCache
    ) {
      AclStoreTask aclStoreTask = new FileAclStoreTask(
          runtimeInfo,
          pipelineStoreTask,
          lockCache,
          Mockito.mock(UserGroupManager.class)
      );
      aclStoreTask.init();
      return aclStoreTask;
    }

    @Provides @Singleton
    public PipelineStateStore providePipelineStateStore(RuntimeInfo runtimeInfo, Configuration configuration) {
      PipelineStateStore pipelineStateStore = new FilePipelineStateStore(runtimeInfo, configuration);
      pipelineStateStore.init();
      return pipelineStateStore;
    }

    @Provides @Singleton
    public StageLibraryTask provideStageLibraryTask() {
      return MockStages.createStageLibrary(new URLClassLoader(new URL[0]));
    }

    @Provides @Singleton @Named("previewExecutor")
    public SafeScheduledExecutorService providePreviewExecutor() {
      return new SafeScheduledExecutorService(1, "preview");
    }

    @Provides @Singleton @Named("runnerExecutor")
    public SafeScheduledExecutorService provideRunnerExecutor() {
      return new SafeScheduledExecutorService(10, "runner");
    }

    @Provides @Singleton @Named("runnerStopExecutor")
    public SafeScheduledExecutorService provideRunnerStopExecutor() {
      return new SafeScheduledExecutorService(10, "runnerStop");
    }

    @Provides @Singleton @Named("managerExecutor")
    public SafeScheduledExecutorService provideManagerExecutor() {
      return new SafeScheduledExecutorService(10, "manager");
    }

    @Provides @Singleton @Named("supportBundleExecutor")
    public SafeScheduledExecutorService provideSupportBundleExecutor() {
      return new SafeScheduledExecutorService(1, "supportBundleExecutor");
    }

    @Provides @Singleton
    public PreviewerProvider providePreviewerProvider() {
      return new PreviewerProvider() {
        @Override
        public Previewer createPreviewer(String user, String name, String rev, PreviewerListener listener,
                                         ObjectGraph objectGraph) {
          Previewer mock = Mockito.mock(Previewer.class);
          Mockito.when(mock.getId()).thenReturn(UUID.randomUUID().toString());
          Mockito.when(mock.getName()).thenReturn(name);
          Mockito.when(mock.getRev()).thenReturn(rev);
          return mock;
        }
      };
    }

    @Provides @Singleton
    public RunnerProvider provideRunnerProvider() {
      return new StandaloneAndClusterRunnerProviderImpl();
    }

    @Provides @Singleton
    public SnapshotStore provideSnapshotStore(RuntimeInfo runtimeInfo, LockCache<String> lockCache) {
      return new FileSnapshotStore(runtimeInfo, lockCache);
    }

    @Provides @Singleton
    public EventListenerManager provideEventListenerManager() {
      return new EventListenerManager();
    }

    @Provides @Singleton
    public LineagePublisherTask provideLineagePublisherTask() {
      return Mockito.mock(LineagePublisherTask.class);
    }

  }

  private void setUpManager(long expiry, long initialThreadExpiryDelay, boolean isDPMEnabled) {
    ObjectGraph objectGraph = ObjectGraph.create(new TestPipelineManagerModule(expiry, initialThreadExpiryDelay));
    RuntimeInfo info = objectGraph.get(RuntimeInfo.class);
    info.setDPMEnabled(isDPMEnabled);
    pipelineStoreTask = objectGraph.get(PipelineStoreTask.class);
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
    pipelineManager.run();
  }

  @Before
  public void setup() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL,
        StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INITIAL_DELAY,
        false);
  }

  @After
  public void tearDown() {
    System.clearProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LIBEXEC_DIR);
    pipelineManager.stop();
    pipelineStoreTask.stop();
  }

  @Test
  public void testPreviewer() throws PipelineException {
    pipelineStoreTask.create("user", "abcd", "label","blah", false, false);
    Previewer previewer = pipelineManager.createPreviewer("user", "abcd", "0");
    assertEquals(previewer, pipelineManager.getPreviewer(previewer.getId()));
    ((StandaloneAndClusterPipelineManager)pipelineManager).outputRetrieved(previewer.getId());
    assertNull(pipelineManager.getPreviewer(previewer.getId()));
  }

  @Test
  public void testPipelineNotExist() {
    try {
      pipelineManager.getRunner("none_existing_pipeline", "0");
      fail("Expected PipelineStoreException but didn't get any");
    } catch (PipelineStoreException ex) {
      LOG.debug("Ignoring exception", ex);
    } catch (Exception ex) {
      fail("Expected PipelineStoreException but got " + ex);
    }
  }

  @Test
  public void testRunner() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "label","blah", false, false);
    Runner runner = pipelineManager.getRunner("aaaa", "0");
    assertNotNull(runner);
  }

  @Test
  public void testGetPipelineStates() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "label","blah", false, false);
    List<PipelineState> pipelineStates = pipelineManager.getPipelines();

    assertEquals("aaaa", pipelineStates.get(0).getPipelineId());
    assertEquals("0", pipelineStates.get(0).getRev());

    pipelineStoreTask.create("user", "bbbb", "label","blah", false, false);
    pipelineStates = pipelineManager.getPipelines();
    assertEquals(2, pipelineStates.size());

    pipelineStoreTask.delete("aaaa");
    pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    pipelineStoreTask.delete("bbbb");
    pipelineStates = pipelineManager.getPipelines();
    assertEquals(0, pipelineStates.size());
  }

  @Test
  public void testRemotePipelineStartup() throws Exception {
    pipelineStoreTask.create("user", "remote", "label","0", true, false);
    pipelineStateStore.saveState("user", "remote", "0", PipelineStatus.CONNECTING, "blah", null, ExecutionMode
            .STANDALONE,
        null, 0, 0);
    pipelineManager.stop();
    pipelineStoreTask.stop();

    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL,
        StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INITIAL_DELAY,
        true);

    await().atMost(Duration.FIVE_SECONDS).until(numPipelinesEqualTo(pipelineManager, 1));
    List<PipelineState> pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    assertTrue(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("remote", "0"));

    pipelineManager.stop();
    pipelineStoreTask.stop();
    pipelineStateStore.saveState("user", "remote", "0", PipelineStatus.CONNECTING, "blah", null, ExecutionMode
        .STANDALONE, null, 0, 0);

    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL,
        StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INITIAL_DELAY,
        false);
    await().atMost(Duration.FIVE_SECONDS).until(numPipelinesEqualTo(pipelineManager, 1));

    pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    // no runner is created
    assertFalse(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("remote", "0"));

  }

  @Test
  public void testInitTask() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "label","blah", false, false);
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.CONNECTING, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineManager.stop();
    pipelineStoreTask.stop();

    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL,
        StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INITIAL_DELAY,
        false);

    await().atMost(Duration.FIVE_SECONDS).until(numPipelinesEqualTo(pipelineManager, 1));

    List<PipelineState> pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    assertTrue(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0"));

    pipelineManager.stop();
    pipelineStoreTask.stop();
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.FINISHING, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);

    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL,
        StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INITIAL_DELAY,
        false);
    await().atMost(Duration.FIVE_SECONDS).until(numPipelinesEqualTo(pipelineManager, 1));

    pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    // no runner is created
    assertFalse(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0"));
  }

  @Test
  public void testExpiry() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "label","blah", false, false);
    Runner runner = pipelineManager.getRunner("aaaa", "0");
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.RUNNING_ERROR, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);
    assertEquals(PipelineStatus.RUNNING_ERROR, runner.getState().getStatus());
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.RUN_ERROR, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);

    pipelineManager.stop();
    pipelineStoreTask.stop();

    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.RUNNING_ERROR, "blah", null, ExecutionMode
        .STANDALONE, null, 0, 0);
    pipelineManager = null;
    setUpManager(100, 0, false);
    await().atMost(Duration.FIVE_SECONDS).until(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0");
      }
    });
  }


  @Test
  public void testPipelineRunnersAtDifferentTimesExpiry() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "label","blah", false, false);
    pipelineStoreTask.create("user", "bbbb", "label","blah", false, false);
    setUpManager(100, 0, false);

    pipelineManager.getRunner("aaaa", "0");
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.STOPPED, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);

    await().atMost(Duration.FIVE_SECONDS).until(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0");
      }
    });

    pipelineManager.getRunner( "aaaa", "0");
    pipelineStateStore.saveState("user", "bbbb", "0", PipelineStatus.STOPPED, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);

    pipelineManager.stop();
    pipelineStoreTask.stop();

    await().atMost(Duration.FIVE_SECONDS).until(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("bbbb", "0");
      }
    });


  }

  @Test
  public void testChangeExecutionModes() throws Exception {
    pipelineStoreTask.create("user1", "pipeline2", "label","blah", false, false);
    pipelineStateStore.saveState("user", "pipeline2", "0", PipelineStatus.EDITED, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);
    Runner runner1 = pipelineManager.getRunner("pipeline2", "0");
    pipelineStateStore.saveState("user", "pipeline2", "0", PipelineStatus.EDITED, "blah", null, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    Runner runner2 = pipelineManager.getRunner("pipeline2", "0");
    assertTrue(runner1 != runner2);
    pipelineStateStore.saveState("user", "pipeline2", "0", PipelineStatus.STARTING, "blah", null, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    pipelineManager.getRunner("pipeline2", "0");
    pipelineStateStore.saveState("user", "pipeline2", "0", PipelineStatus.STARTING, "blah", null, ExecutionMode.STANDALONE, null, 0, 0);
    try {
      pipelineManager.getRunner("pipeline2", "0");
      fail("Expected exception but didn't get any");
    } catch (PipelineManagerException pme) {
      // Expected
    }
  }

}
