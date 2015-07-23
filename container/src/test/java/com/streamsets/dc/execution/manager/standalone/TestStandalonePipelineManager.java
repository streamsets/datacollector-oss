/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.manager.standalone;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dc.execution.EventListenerManager;
import com.streamsets.dc.execution.Manager;
import com.streamsets.dc.execution.PipelineState;
import com.streamsets.dc.execution.PipelineStateStore;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.PreviewerListener;
import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.SnapshotStore;
import com.streamsets.dc.execution.manager.PreviewerProvider;
import com.streamsets.dc.execution.manager.RunnerProvider;
import com.streamsets.dc.execution.runner.provider.StandaloneAndClusterRunnerProviderImpl;
import com.streamsets.dc.execution.runner.standalone.StandaloneRunner;
import com.streamsets.dc.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.dc.execution.store.FilePipelineStateStore;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.FilePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.LockCache;
import com.streamsets.pipeline.util.LockCacheModule;

import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Named;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestStandalonePipelineManager {

  private PipelineStoreTask pipelineStoreTask;
  private Manager pipelineManager;
  private PipelineStateStore pipelineStateStore;

  @Module(injects = {StandaloneAndClusterPipelineManager.class, PipelineStoreTask.class, PipelineStateStore.class,
    StandaloneRunner.class, EventListenerManager.class, LockCache.class},  includes = LockCacheModule.class,
    library = true)
  public static class TestPipelineManagerModule {

    private final long expiry;

    public TestPipelineManagerModule(long expiry) {
      this.expiry = expiry;
    }

    @Provides @Singleton
    public RuntimeInfo providesRuntimeInfo() {
      return new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestStandalonePipelineManager.class.getClassLoader()));
    }

    @Provides @Singleton
    public Configuration provideConfiguration() {
      Configuration configuration = new Configuration();
      configuration.set(StandaloneAndClusterPipelineManager.RUNNER_EXPIRY_INTERVAL, expiry);
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

    @Provides @Singleton @Named("managerExecutor")
    public SafeScheduledExecutorService provideManagerExecutor() {
      return new SafeScheduledExecutorService(10, "manager");
    }

    @Provides @Singleton @Named("asyncExecutor")
    public SafeScheduledExecutorService provideAsyncExecutor() {
      return new SafeScheduledExecutorService(1, "asyncExecutor");
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
    public SnapshotStore provideSnapshotStore(RuntimeInfo runtimeInfo) {
      return new FileSnapshotStore(runtimeInfo);
    }

    @Provides @Singleton
    public EventListenerManager provideEventListenerManager() {
      return new EventListenerManager();
    }

  }

  private void setUpManager(long expiry) {
    ObjectGraph objectGraph = ObjectGraph.create(new TestPipelineManagerModule(expiry));
    pipelineStoreTask = objectGraph.get(PipelineStoreTask.class);
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineManager = new StandaloneAndClusterPipelineManager(objectGraph);
    pipelineManager.init();
  }

  @Before
  public void setup() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL);
  }

  @After
  public void tearDown() {
    pipelineManager.stop();
    pipelineStoreTask.stop();
  }

  @Test
  public void testPreviewer() {
    Previewer previewer = pipelineManager.createPreviewer("user", "abcd", "0");
    assertEquals(previewer, pipelineManager.getPreview(previewer.getId()));
    ((StandaloneAndClusterPipelineManager)pipelineManager).outputRetrieved(previewer.getId());
    assertNull(pipelineManager.getPreview(previewer.getId()));
  }

  @Test
  public void testRunner() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "blah");
    Runner runner = pipelineManager.getRunner("user1", "aaaa", "0");
    assertNotNull(runner);
  }

  @Test
  public void testGetPipelineStates() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "blah");
    List<PipelineState> pipelineStates = pipelineManager.getPipelines();

    assertEquals("aaaa", pipelineStates.get(0).getName());
    assertEquals("0", pipelineStates.get(0).getRev());

    pipelineStoreTask.create("user", "bbbb", "blah");
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
  public void testInitTask() throws Exception {

    pipelineStoreTask.create("user", "aaaa", "blah");
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.CONNECTING, "blah", null, ExecutionMode.STANDALONE);

    pipelineManager.stop();
    pipelineStoreTask.stop();

    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL);
    Thread.sleep(2000);
    List<PipelineState> pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    assertTrue(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0"));
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.FINISHING, "blah", null, ExecutionMode.STANDALONE);

    pipelineManager.stop();
    pipelineStoreTask.stop();

    setUpManager(StandaloneAndClusterPipelineManager.DEFAULT_RUNNER_EXPIRY_INTERVAL);
    Thread.sleep(2000);

    pipelineStates = pipelineManager.getPipelines();
    assertEquals(1, pipelineStates.size());
    assertEquals(PipelineStatus.FINISHED, pipelineStates.get(0).getStatus());
    // no runner is created
    assertFalse(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0"));
  }

  @Test
  public void testExpiry() throws Exception {
    pipelineStoreTask.create("user", "aaaa", "blah");
    Runner runner = pipelineManager.getRunner("user1", "aaaa", "0");
    pipelineStateStore.saveState("user", "aaaa", "0", PipelineStatus.RUNNING_ERROR, "blah", null, ExecutionMode.STANDALONE);
    assertEquals(PipelineStatus.RUNNING_ERROR, runner.getState().getStatus());

    pipelineManager.stop();
    pipelineStoreTask.stop();

    pipelineManager = null;
    setUpManager(100);
    Thread.sleep(2000);
    assertFalse(((StandaloneAndClusterPipelineManager) pipelineManager).isRunnerPresent("aaaa", "0"));
  }

}
