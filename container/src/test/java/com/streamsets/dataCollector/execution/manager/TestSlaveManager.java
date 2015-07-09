/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dataCollector.execution.Manager;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.Runner;
import com.streamsets.dataCollector.execution.runner.RunnerProviderImpl;
import com.streamsets.dataCollector.execution.runner.StandaloneRunner;
import com.streamsets.dataCollector.execution.store.FilePipelineStateStore;
import com.streamsets.dataCollector.execution.store.SlavePipelineStateStore;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.snapshotstore.SnapshotStore;
import com.streamsets.pipeline.snapshotstore.impl.FileSnapshotStore;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.store.impl.SlavePipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.TestUtil;

import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

public class TestSlaveManager {

  private Manager manager;

  @Module(
    injects = { SlaveManager.class, PipelineStoreTask.class, PipelineStateStore.class, StandaloneRunner.class },
    library = true)
  public static class TestSlaveManagerModule {

    public TestSlaveManagerModule() {
    }

    @Provides
    @Singleton
    public RuntimeInfo providesRuntimeInfo() {
      return new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestSlaveManager.class.getClassLoader()));
    }

    @Provides
    @Singleton
    public Configuration provideConfiguration() {
      Configuration configuration = new Configuration();
      configuration.set(Runner.CALLBACK_SERVER_URL_KEY, "/dummy/v1");
      return configuration;
    }

    @Provides
    @Singleton
    public PipelineStoreTask providePipelineStoreTask(RuntimeInfo runtimeInfo, StageLibraryTask stageLibraryTask,
      PipelineStateStore pipelineStateStore) {
      PipelineStoreTask pipelineStoreTask =
        new SlavePipelineStoreTask(new TestUtil.TestPipelineStoreModuleNew().providePipelineStore(runtimeInfo,
          stageLibraryTask, new FilePipelineStateStore(runtimeInfo, new Configuration())));
      return pipelineStoreTask;
    }

    @Provides
    @Singleton
    public PipelineStateStore providePipelineStateStore(RuntimeInfo runtimeInfo, Configuration configuration) {
      PipelineStateStore pipelineStateStore = new SlavePipelineStateStore();
      return pipelineStateStore;
    }

    @Provides
    @Singleton
    public StageLibraryTask provideStageLibraryTask() {
      return MockStages.createStageLibrary(new URLClassLoader(new URL[0]));
    }

    @Provides
    @Singleton
    @Named("runnerExecutor")
    public SafeScheduledExecutorService provideRunnerExecutor() {
      return new SafeScheduledExecutorService(10, "runner");
    }

    @Provides
    @Singleton
    public SnapshotStore provideSnapshotStore(RuntimeInfo runtimeInfo) {
      return new FileSnapshotStore(runtimeInfo);
    }

    @Provides
    @Singleton
    public RunnerProvider provideRunnerProvider() {
      return new RunnerProviderImpl();
    }
  }

  private void setUpManager() {
    ObjectGraph objectGraph = ObjectGraph.create(new TestSlaveManagerModule());
    manager = new SlaveManager(objectGraph);
    manager.init();
  }

  @Before
  public void setup() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    try {
      FileUtils.deleteDirectory(f);
    } catch (Exception ex) {
      // ok
    }
    setUpManager();
  }

  @After
  public void tearDown() {
    manager.stop();
  }

  @Test
  public void testGetRunner() throws Exception {
    Runner runner = manager.getRunner("user1", "pipe1", "0");
    try {
      runner.resetOffset();
      fail("Expected exception but didn't get any");
    } catch (UnsupportedOperationException e) {
      //expected
    }
    assertTrue(!manager.isPipelineActive("pipe1", "0"));
    Runner runner2 = manager.getRunner("user1", "pipe1", "0");
    assertTrue (runner == runner2);

    try {
      manager.getRunner("user1", "pipe2", "0");
      fail("Expected exception but didn't get any");
    } catch (IllegalStateException e) {
      //expected
    }
  }

}
