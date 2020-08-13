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
package com.streamsets.datacollector.execution.store;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.SlaveRuntimeInfo;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.LockCacheModule;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.TestUtil;
import com.streamsets.datacollector.util.credential.PipelineCredentialHandler;
import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.api.ExecutionMode;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPipelineStateStore {

  private static PipelineStateStore pipelineStateStore;
  private static PipelineStoreTask pipelineStoreTask;
  private static RuntimeInfo runtimeInfo;

  static class MockFilePipelineStateStore extends CachePipelineStateStore {

    @Inject
    public MockFilePipelineStateStore(PipelineStateStore pipelineStateStore, Configuration configuration) {
      super(pipelineStateStore, configuration);
    }

    static boolean INVALIDATE_CACHE = false;

    @Override
    public PipelineState edited(
        String user,
        String name,
        String rev,
        ExecutionMode executionMode,
        boolean isRemote,
        Map<String, Object> metadata
    ) throws PipelineStoreException {
      PipelineState state = super.edited(user, name, rev, executionMode, isRemote, metadata);
      if (INVALIDATE_CACHE) {
        // invalidate cache
        super.destroy();
      }
      return state;
    }

    @Override
    public PipelineState saveState(String user, String name, String rev, PipelineStatus status, String message,
                                   Map<String, Object> attributes, ExecutionMode executionMode, String metrics, int retryAttempt, long nextRetryTimeStamp) throws PipelineStoreException {
      if (INVALIDATE_CACHE) {
        super.destroy();
      }
      return super.saveState(user, name, rev, status, message, attributes, executionMode, metrics, retryAttempt, nextRetryTimeStamp);
    }

  }

  @Module(injects = {PipelineStateStore.class, PipelineStoreTask.class, SlaveRuntimeInfo.class}, library = true,
    includes = {TestUtil.TestStageLibraryModule.class, LockCacheModule.class})
  static class TestPipelineStateStoreModule {

    @Provides @Singleton
    public BuildInfo provideBuildInfo() {
      return ProductBuildInfo.getDefault();
    }

    @Provides @Singleton
    public SlaveRuntimeInfo provideRuntimeInfo() {
      return new SlaveRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
        ImmutableList.of(getClass().getClassLoader()));
    }

    @Provides @Singleton
    public Configuration provideConfiguration() {
      return new Configuration();
    }

    @Provides
    @Singleton
    public EventListenerManager provideEventListenerManager() {
      return Mockito.spy(new EventListenerManager());
    }

    @Provides @Singleton
    public PipelineStateStore providePipelineStateStore(SlaveRuntimeInfo runtimeInfo, Configuration configuration) {
      return new MockFilePipelineStateStore(new FilePipelineStateStore(runtimeInfo, configuration), configuration);
    }

    @Provides
    @Singleton
    public PipelineStoreTask providePipelineStore(
        BuildInfo buildInfo,
        Configuration configuration,
        SlaveRuntimeInfo slaveRuntimeInfo,
        StageLibraryTask stageLibraryTask,
        PipelineStateStore pipelineStateStore,
        EventListenerManager eventListenerManager,
        LockCache<String> lockCache
    ) {
      return new FilePipelineStoreTask(
          buildInfo,
          slaveRuntimeInfo,
          stageLibraryTask,
          pipelineStateStore,
          eventListenerManager,
          lockCache,
          Mockito.mock(PipelineCredentialHandler.class),
          configuration
      );
    }
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    TestUtil.captureMockStages();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before()
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    ObjectGraph objectGraph = ObjectGraph.create(TestPipelineStateStoreModule.class);
    pipelineStateStore = objectGraph.get(PipelineStateStore.class);
    pipelineStoreTask = objectGraph.get(PipelineStoreTask.class);
    runtimeInfo = objectGraph.get(SlaveRuntimeInfo.class);
    pipelineStoreTask.init();
  }

  private PipelineConfiguration createPipeline(UUID uuid) {
    PipelineConfiguration pc = MockStages.createPipelineConfigurationWithClusterOnlyStage(ExecutionMode.CLUSTER_BATCH);
    pc.setUuid(uuid);
    return pc;
  }

  @After
  public void tearDown() {
    pipelineStoreTask.stop();
  }

  @Test
  public void testCreatePipeline() throws Exception {
    pipelineStoreTask.create("user2", "name1", "label", "description", false, false, new HashMap<String, Object>());
    PipelineState pipelineState = pipelineStateStore.getState("name1", "0");

    assertEquals("user2", pipelineState.getUser());
    assertEquals("name1", pipelineState.getPipelineId());
    assertEquals("0", pipelineState.getRev());
    assertEquals(ExecutionMode.STANDALONE, pipelineState.getExecutionMode());

    PipelineConfiguration pc0 = pipelineStoreTask.load("name1", "0");
    pc0 = createPipeline(pc0.getUuid());
    pipelineStoreTask.save("user3", "name1", "0", "execution mdoe changed", pc0, false);
    pipelineState = pipelineStateStore.getState("name1", "0");
    assertEquals("user3", pipelineState.getUser());
    assertEquals("name1", pipelineState.getPipelineId());
    assertEquals("0", pipelineState.getRev());
    assertEquals(ExecutionMode.CLUSTER_BATCH, pipelineState.getExecutionMode());

    pc0 = pipelineStoreTask.load("name1", "0");
    pc0 = createPipeline(pc0.getUuid());
    pipelineStoreTask.save("user4", "name1", "0", "execution mdoe same", pc0, false);
    pipelineState = pipelineStateStore.getState("name1", "0");
    // should still be user3 as we dont persist state file on each edit (unless the execution mode has changed)
    assertEquals("user3", pipelineState.getUser());
  }

  @Test
  public void testStateSaveNoCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = true;
    stateSave();
  }

  @Test
  public void testStateSaveCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = false;
    stateSave();
  }

  @Test
  public void testStateEditNoCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = true;
    stateEdit();
  }

  @Test
  public void testStateEditCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = false;
    stateEdit();
  }

  @Test
  public void testStateDeleteNoCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = true;
    stateDelete();
  }

  @Test
  public void testStateDeleteCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = false;
    stateDelete();
  }

  @Test
  public void stateHistory() throws Exception {
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "aaa", "0").toPath());
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.RUNNING, "Pipeline stopped", null, ExecutionMode.STANDALONE, null, 0, 0);
    List<PipelineState> history = pipelineStateStore.getHistory("aaa", "0", true);
    for (PipelineState pipelineState: history) {
      assertEquals(PipelineStatus.RUNNING, pipelineState.getStatus());
      assertEquals(PipelineStatus.STOPPED, pipelineState.getStatus());
    }
  }

  @Test
  public void stateChangeExecutionMode() throws Exception {
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "aaa", "0").toPath());
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null, ExecutionMode.CLUSTER_BATCH, null, 0, 0);
    PipelineState pipelineState = pipelineStateStore.getState("aaa", "0");
    assertEquals(ExecutionMode.CLUSTER_BATCH, pipelineState.getExecutionMode());
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineState = pipelineStateStore.getState("aaa", "0");
    assertEquals(ExecutionMode.STANDALONE, pipelineState.getExecutionMode());
  }

  @Test
  public void testStateRemoteAttribute() throws Exception {
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "aaa", "0").toPath());
    pipelineStateStore.edited("user2", "stateRemoteAttribute", "0", ExecutionMode.STANDALONE, true,
        new HashMap<String, Object>()
    );
    PipelineState pipelineState = pipelineStateStore.getState("stateRemoteAttribute", "0");
    assertEquals(true, pipelineState.getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE));
    pipelineStateStore.saveState("user2", "stateRemoteAttribute", "0", PipelineStatus.STOPPED, "Pipeline starting", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineStateStore.edited("user2", "stateRemoteAttribute", "0", ExecutionMode.CLUSTER_BATCH, false,
        new HashMap<String, Object>()
    );
    pipelineState = pipelineStateStore.getState("stateRemoteAttribute", "0");
    assertEquals(true, pipelineState.getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE));
    assertEquals(ExecutionMode.CLUSTER_BATCH, pipelineState.getExecutionMode());

    pipelineStateStore.edited("user2", "stateRemoteAttribute1", "0", ExecutionMode.STANDALONE, false,
        new HashMap<String, Object>()
    );
    pipelineState = pipelineStateStore.getState("stateRemoteAttribute1", "0");
    assertEquals(false, pipelineState.getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE));
    pipelineStateStore.saveState("user1", "stateRemoteAttribute2", "0", PipelineStatus.EDITED, "Pipeline edited", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineState = pipelineStateStore.getState("stateRemoteAttribute2", "0");
    assertEquals(false, pipelineStoreTask.isRemotePipeline("stateRemoteAttribute2", "0"));

  }

  public void stateSave() throws Exception {
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "aaa", "0").toPath());
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.EDITED, "Pipeline edited", null, ExecutionMode.STANDALONE, null, 0, 0);
    PipelineState pipelineState = pipelineStateStore.getState("aaa", "0");
    assertEquals("user1", pipelineState.getUser());
    assertEquals("aaa", pipelineState.getPipelineId());
    assertEquals("0", pipelineState.getRev());
    assertEquals(PipelineStatus.EDITED, pipelineState.getStatus());
    assertEquals("Pipeline edited", pipelineState.getMessage());
    assertEquals(ExecutionMode.STANDALONE, pipelineState.getExecutionMode());
  }

  public void stateDelete() throws Exception {
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "aaa", "0").toPath());
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineStateStore.delete("aaa", "0");
    try {
      pipelineStateStore.getState("aaa", "0");
      fail("Expected exception but didn't get any");
    } catch (PipelineStoreException ex) {
      // expected
    }
  }

  public void stateEdit() throws Exception {
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "aaa", "0").toPath());
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null, ExecutionMode.STANDALONE, null, 0, 0);
    pipelineStateStore.edited("user2", "aaa", "0", ExecutionMode.STANDALONE, false, new HashMap<String, Object>());
    PipelineState pipelineState = pipelineStateStore.getState("aaa", "0");
    assertEquals("user2", pipelineState.getUser());
    assertEquals("aaa", pipelineState.getPipelineId());
    assertEquals("0", pipelineState.getRev());
    assertEquals(PipelineStatus.EDITED, pipelineState.getStatus());

    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.RUNNING, "Pipeline running", null, ExecutionMode.STANDALONE, null, 0, 0);
    try {
      pipelineStateStore.edited("user2", "aaa", "0", ExecutionMode.STANDALONE, false, new HashMap<String, Object>());
      fail("Expected exception but didn't get any");
    } catch (IllegalStateException ex) {
      // expected
    }
  }

}
