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
package com.streamsets.datacollector.execution.manager;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.execution.runner.provider.SlaveRunnerProviderImpl;
import com.streamsets.datacollector.execution.store.FilePipelineStateStore;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.store.impl.SlavePipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSlaveManager {

  private Manager manager;

  @Module(
    injects = {
        SlavePipelineManager.class,
        PipelineStoreTask.class,
        PipelineStateStore.class,
        EventListenerManager.class,
        AclStoreTask.class
    },
    library = true)
  public static class TestSlaveManagerModule {

    public TestSlaveManagerModule() {
    }

    @Provides
    @Singleton
    public RuntimeInfo providesRuntimeInfo() {
      return new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        Arrays.asList(TestSlaveManager.class.getClassLoader()));
    }

    @Provides
    @Singleton
    public Configuration provideConfiguration() {
      Configuration configuration = new Configuration();
      configuration.set(Constants.PIPELINE_CLUSTER_TOKEN_KEY, "sdcClusterToken");
      configuration.set(Constants.CALLBACK_SERVER_URL_KEY, "/dummy/v1");
      return configuration;
    }

    @Provides
    @Singleton
    public PipelineStoreTask providePipelineStoreTask(RuntimeInfo runtimeInfo, StageLibraryTask stageLibraryTask,
      PipelineStateStore pipelineStateStore) {
      PipelineStoreTask pipelineStoreTask =
        new SlavePipelineStoreTask(new TestUtil.TestPipelineStoreModuleNew().providePipelineStore(runtimeInfo,
          stageLibraryTask, new FilePipelineStateStore(runtimeInfo, provideConfiguration())));
      return pipelineStoreTask;
    }

    @Provides
    @Singleton
    public AclStoreTask provideAclStoreTask(RuntimeInfo runtimeInfo, PipelineStoreTask pipelineStoreTask) {
      AclStoreTask aclStoreTask = new FileAclStoreTask(
          runtimeInfo,
          pipelineStoreTask,
          new LockCache<String>(),
          Mockito.mock(UserGroupManager.class)
      );
      aclStoreTask.init();
      return aclStoreTask;
    }

    @Provides
    @Singleton
    public PipelineStateStore providePipelineStateStore() {
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
    @Named("runnerStopExecutor")
    public SafeScheduledExecutorService provideRunnerStopExecutor() {
      return new SafeScheduledExecutorService(10, "runnerStop");
    }

    @Provides
    @Singleton
    public RunnerProvider provideRunnerProvider() {
      return new SlaveRunnerProviderImpl();
    }

    @Provides @Singleton
    public SnapshotStore provideSnapshotStore() {
      return Mockito.mock(SnapshotStore.class);
    }

    @Provides @Singleton
    public EventListenerManager provideEventListenerManager() {
      return new EventListenerManager();
    }

  }

  private void setUpManager() {
    ObjectGraph objectGraph = ObjectGraph.create(new TestSlaveManagerModule());
    manager = new SlavePipelineManager(objectGraph);
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
    Runner runner = manager.getRunner(TestUtil.MY_PIPELINE, TestUtil.ZERO_REV);
    try {
      runner.resetOffset("admin");
      fail("Expected exception but didn't get any");
    } catch (UnsupportedOperationException e) {
      //expected
    }
    assertTrue(!manager.isPipelineActive(TestUtil.MY_PIPELINE, TestUtil.ZERO_REV));
    Runner runner2 = manager.getRunner(TestUtil.MY_PIPELINE, TestUtil.ZERO_REV);
    assertTrue (runner == runner2);

    try {
      manager.getRunner("pipe2", TestUtil.ZERO_REV);
      fail("Expected exception but didn't get any");
    } catch (IllegalStateException e) {
      //expected
    }
  }

}
