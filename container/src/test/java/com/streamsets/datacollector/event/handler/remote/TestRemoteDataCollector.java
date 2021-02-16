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
package com.streamsets.datacollector.event.handler.remote;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.callback.CallbackObjectType;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineFragmentConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.dto.ValidationStatus;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.preview.common.PreviewOutputImpl;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.SampledRecord;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.production.OffsetFileUtil;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.runner.production.SourceOffsetUpgrader;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineRevInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.Issues;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

public class TestRemoteDataCollector {

  @Rule
  public TemporaryFolder tempFolder= new TemporaryFolder();

  public Object afterActionsFunctionParam;

  @Before
  public void resetState() {
    this.afterActionsFunctionParam =  null;
  }

  private static class MockManager implements Manager {

    Map<String, Previewer> map = new HashMap<>();
    Map<String, PipelineState> stateMap = new HashMap<>();
    private MockPreviewer lastPreviewer = null;

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void init() {
      // TODO Auto-generated method stub

    }

    @Override
    public void run() {
      // TODO Auto-generated method stub

    }

    @Override
    public void waitWhileRunning() throws InterruptedException {
      // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
      // TODO Auto-generated method stub
    }

    @Override
    public Status getStatus() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Previewer createPreviewer(
        String user,
        String name,
        String rev,
        List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
        Function<Object, Void> afterActionsFunction,
        boolean remote,
        Map<String, ConnectionConfiguration> connections
    ) throws PipelineStoreException {
      final MockPreviewer mockPreviewer = new MockPreviewer(user, name, rev, interceptorConfs, afterActionsFunction);
      Previewer previewer = mockPreviewer;
      map.put(previewer.getId(), previewer);
      this.lastPreviewer = mockPreviewer;
      return previewer;
    }

    MockPreviewer getLastPreviewer() {
      return lastPreviewer;
    }

    @Override
    public Previewer getPreviewer(String previewerId) {
      return map.get(previewerId);
    }

    @Override
    public Runner getRunner(
        String name,
        String rev
    ) throws PipelineStoreException, PipelineManagerException {
      return new MockRunner();
    }

    @Override
    public List<PipelineState> getPipelines() throws PipelineStoreException {
      // Create 3 remote pipelines
      PipelineState pipelineStatus1 = new PipelineStateImpl("user",
          "ns:name",
          "rev",
          PipelineStatus.EDITED,
          null,
          System.currentTimeMillis(),
          null,
          ExecutionMode.STANDALONE,
          null,
          0,
          -1
      );
      PipelineState pipelineStatus2 = new PipelineStateImpl("user1",
          "ns:name1",
          "rev1",
          PipelineStatus.EDITED,
          null,
          System.currentTimeMillis(),
          null,
          ExecutionMode.STANDALONE,
          null,
          0,
          -1
      );
      PipelineState pipelineStatus3 = new PipelineStateImpl("user1",
          "ns:name2",
          "rev1",
          PipelineStatus.RUNNING,
          null,
          System.currentTimeMillis(),
          null,
          ExecutionMode.STANDALONE,
          null,
          0,
          -1
      );
      // create one local pipeline in active state
      PipelineState pipelineStatus4 = new PipelineStateImpl("user1",
          "local",
          "rev1",
          PipelineStatus.RUNNING,
          null,
          System.currentTimeMillis(),
          null,
          ExecutionMode.STANDALONE,
          null,
          0,
          -1
      );
      // create one local pipeline in non active state
      PipelineState pipelineStatus5 = new PipelineStateImpl("user1",
          "localError",
          "rev1",
          PipelineStatus.RUN_ERROR,
          null,
          System.currentTimeMillis(),
          null,
          ExecutionMode.STANDALONE,
          null,
          0,
          -1
      );
      List<PipelineState> pipelineList = new ArrayList<PipelineState>();
      pipelineList.add(pipelineStatus1);
      pipelineList.add(pipelineStatus2);
      pipelineList.add(pipelineStatus3);
      pipelineList.add(pipelineStatus4);
      pipelineList.add(pipelineStatus5);
      stateMap.put(pipelineStatus1.getPipelineId() + "::" + pipelineStatus1.getRev(), pipelineStatus1);
      stateMap.put(pipelineStatus2.getPipelineId() + "::" + pipelineStatus2.getRev(), pipelineStatus2);
      stateMap.put(pipelineStatus3.getPipelineId() + "::" + pipelineStatus3.getRev(), pipelineStatus3);
      stateMap.put(pipelineStatus4.getPipelineId() + "::" + pipelineStatus4.getRev(), pipelineStatus4);
      stateMap.put(pipelineStatus5.getPipelineId() + "::" + pipelineStatus5.getRev(), pipelineStatus5);
      return pipelineList;
    }

    public PipelineState getPipelineState(String name, String rev) throws PipelineStoreException {
      return stateMap.get(name + "::" + rev);
    }

    @Override
    public boolean isPipelineActive(String name, String rev) throws PipelineStoreException {
      return stateMap.get(name + "::" + rev).getStatus().isActive();
    }

    @Override
    public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
      return (name.contains(":") ? true : false);
    }

    @Override
    public void addStateEventListener(StateEventListener listener) {
      //
    }
  }

  public static class MockRunner implements Runner {

    public static int stopCalled;

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getRev() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return null;
    }

    @Override
    public String getPipelineTitle() throws PipelineException {
      return null;
    }

    @Override
    public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
      return null;
    }

    @Override
    public void resetOffset(String user) throws PipelineStoreException, PipelineRunnerException {
      // TODO Auto-generated method stub

    }

    @Override
    public SourceOffset getCommittedOffsets() throws PipelineException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void updateCommittedOffsets(SourceOffset sourceOffset) throws PipelineException {
    }

    @Override
    public PipelineState getState() throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void prepareForDataCollectorStart(String user) throws PipelineStoreException, PipelineRunnerException {
      // TODO Auto-generated method stub

    }

    @Override
    public void onDataCollectorStart(String user) throws PipelineException, StageException {
      // TODO Auto-generated method stub

    }

    @Override
    public void onDataCollectorStop(String user) throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
      // TODO Auto-generated method stub

    }

    @Override
    public void stop(String user) throws PipelineException {
      stopCalled++;
    }

    @Override
    public void forceQuit(String user) throws PipelineException {
      // No-op
    }

    @Override
    public void prepareForStart(StartPipelineContext context) throws PipelineException {

    }

    @Override
    public void prepareForStop(String user) throws PipelineStoreException, PipelineRunnerException {
      // TODO Auto-generated method stub

    }

    @Override
    public void start(StartPipelineContext context) throws PipelineException, StageException {

    }

    @Override
    public void startAndCaptureSnapshot(StartPipelineContext context, String snapshotName, String snapshotLabel, int batches, int batchSize) throws PipelineException, StageException {

    }

    @Override
    public String captureSnapshot(
        String user,
        String snapshotName,
        String snapshotLabel,
        int batches,
        int batchSize
    ) throws PipelineException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String updateSnapshotLabel(String snapshotName, String snapshotLabel) throws PipelineException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Snapshot getSnapshot(String id) throws PipelineException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<SnapshotInfo> getSnapshotsInfo() throws PipelineException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void deleteSnapshot(String id) throws PipelineException {
      // TODO Auto-generated method stub

    }

    @Override
    public List<PipelineState> getHistory() throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void deleteHistory() {
      // TODO Auto-generated method stub

    }

    @Override
    public Object getMetrics() throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<Record> getErrorRecords(String stage, int max) throws PipelineRunnerException, PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<ErrorMessage> getErrorMessages(
        String stage,
        int max
    ) throws PipelineRunnerException, PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<SampledRecord> getSampledRecords(
        String sampleId,
        int max
    ) throws PipelineRunnerException, PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<AlertInfo> getAlerts() throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Collection<CallbackInfo> getSlaveCallbackList(CallbackObjectType callbackObjectType) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Map<String, Object> createStateAttributes() {
      return new HashMap<>();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
      return null;
    }

    @Override
    public String getToken() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getRunnerCount() {
      return 0;
    }

    @Override
    public Runner getDelegatingRunner() {
      return null;
    }
  }

  private static class MockPipelineStateStore implements PipelineStateStore {

    public static int getStateCalled;

    @Override
    public PipelineState edited(
        String user,
        String name,
        String rev,
        ExecutionMode executionMode,
        boolean isRemote, Map<String, Object> metadata
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void delete(String name, String rev) throws PipelineStoreException {
      // TODO Auto-generated method stub

    }

    @Override
    public PipelineState saveState(
        String user,
        String name,
        String rev,
        PipelineStatus status,
        String message,
        Map<String, Object> attributes,
        ExecutionMode executionMode,
        String metrics,
        int retryAttempt,
        long nextRetryTimeStamp
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PipelineState getState(String name, String rev) throws PipelineStoreException {
      if (getStateCalled == 1) {
        getStateCalled++;
        return new PipelineStateImpl(
            "user",
            name,
            rev,
            PipelineStatus.STOPPED,
            null,
            -1,
            null,
            ExecutionMode.STANDALONE,
            null,
            -1,
            -1
        );
      } else {
        getStateCalled++;
        return new PipelineStateImpl(
            "user",
            name,
            rev,
            PipelineStatus.RUNNING,
            null,
            -1,
            null,
            ExecutionMode.STANDALONE,
            null,
            -1,
            -1
        );
      }
    }

    @Override
    public List<PipelineState> getHistory(
        String name,
        String rev,
        boolean fromBeginning
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void deleteHistory(String name, String rev) {
      // TODO Auto-generated method stub

    }

    @Override
    public void init() {
      // TODO Auto-generated method stub

    }

    @Override
    public void destroy() {
      // TODO Auto-generated method stub
    }

  }

  private static class MockPipelineStoreTask implements PipelineStoreTask {

    public static int deleteCalled;
    public static int deleteRulesCalled;

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void init() {
      // TODO Auto-generated method stub

    }

    @Override
    public void run() {
      // TODO Auto-generated method stub

    }

    @Override
    public void waitWhileRunning() throws InterruptedException {
      // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
      // TODO Auto-generated method stub

    }

    @Override
    public Status getStatus() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PipelineConfiguration create(
        String user,
        String pipelineId,
        String pipelineTitle,
        String description,
        boolean isRemote,
        boolean draft,
        Map<String, Object> metadata
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return new PipelineConfiguration(
          1,
          1,
          "pipelineId",
          UUID.randomUUID(),
          "label",
          "",
          Arrays.asList(new Config("", "")),
          null,
          null,
          null,
          null,
          Collections.emptyList(),
          Collections.emptyList()
      );
    }

    @Override
    public void delete(String name) throws PipelineStoreException {
      deleteCalled++;
    }

    @Override
    public List<PipelineInfo> getPipelines() throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PipelineInfo getInfo(String name) throws PipelineStoreException {
      return new PipelineInfo(name, "title", null, null, null, null, null, null, null, false, null, null, null);
    }

    @Override
    public List<PipelineRevInfo> getHistory(String name) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PipelineConfiguration load(String name, String tagOrRev) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean hasPipeline(String name) {
      if (deleteCalled == 0) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public RuleDefinitions retrieveRules(String name, String tagOrRev) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RuleDefinitions storeRules(
        String pipelineName,
        String tag,
        RuleDefinitions ruleDefinitions,
        boolean draft
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean deleteRules(String name) throws PipelineStoreException {
      deleteRulesCalled++;
      return false;
    }

    @Override
    public void saveUiInfo(String name, String rev, Map<String, Object> uiInfo) throws PipelineStoreException {
      // TODO Auto-generated method stub

    }

    @Override
    public PipelineConfiguration saveMetadata(
        String user,
        String name,
        String rev,
        Map<String, Object> metadata
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void registerStateListener(StateEventListener stateListener) {

    }

    @Override
    public PipelineFragmentConfiguration createPipelineFragment(
        String user,
        String pipelineId,
        String pipelineTitle,
        String description,
        boolean draft
    ) throws PipelineException {
      return null;
    }

    @Override
    public PipelineConfiguration save(
        String user,
        String name,
        String tag,
        String tagDescription,
        PipelineConfiguration pipeline,
        boolean encryptCredentials
    ) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean isRemotePipeline(String name, String rev) throws PipelineStoreException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public List<PipelineInfo> getSamplePipelines() {
      return null;
    }

    @Override
    public PipelineEnvelopeJson loadSamplePipeline(String samplePipelineId) {
      return null;
    }

  }

  private void mockDpmClientInfo(RuntimeInfo runtimeInfoMock) {
    Mockito.when(runtimeInfoMock.getAttribute(Mockito.eq(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)))
        .thenReturn(new DpmClientInfo() {
          @Override
          public String getDpmBaseUrl() {
            return "http://localhost:18631";
          }

          @Override
          public Map<String, String> getHeaders() {
            return ImmutableMap.of(
                SSOConstants.X_APP_COMPONENT_ID, "componentId",
                SSOConstants.X_APP_AUTH_TOKEN, "authToken"
            );
          }

          @Override
          public void setDpmBaseUrl(String dpmBaseUrl) {

          }
        });
  }

  @Test
  public void testValidateConfigs() throws Exception {
    try {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      Mockito.when(runtimeInfo.getAppAuthToken()).thenReturn("");
      Mockito.when(runtimeInfo.getId()).thenReturn("fakeId");
      mockDpmClientInfo(runtimeInfo);
      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
      AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
      RemoteDataCollector dataCollector = new RemoteDataCollector(
          new Configuration(),
          new MockManager(),
          new MockPipelineStoreTask(),
          new MockPipelineStateStore(),
          aclStoreTask,
          new RemoteStateEventListener(new Configuration()),
          runtimeInfo,
          buildInfo,
          Mockito.mock(AclCacheHelper.class),
          Mockito.mock(StageLibraryTask.class),
          Mockito.mock(BlobStoreTask.class),
          new SafeScheduledExecutorService(1, "supportBundleExecutor")
      );
      dataCollector.validateConfigs("user", "ns:name", "rev", Collections.emptyList());
      dataCollector.validateConfigs("user1", "ns:name1", "rev1", Collections.emptyList());
      assertEquals(2, MockPreviewer.validateConfigsCalled);
      assertEquals("user" + "ns:name" + "rev", dataCollector.getValidatorList().get(0));
      assertEquals("user1" + "ns:name1" + "rev1", dataCollector.getValidatorList().get(1));
    } finally {
      MockPreviewer.validateConfigsCalled = 0;
    }
  }

  @Test
  public void testStopAndDelete() throws Exception {
    try {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      Mockito.when(runtimeInfo.getAppAuthToken()).thenReturn("");
      Mockito.when(runtimeInfo.getId()).thenReturn("fakeId");
      mockDpmClientInfo(runtimeInfo);
      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
      AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
      Configuration configuration = new Configuration();
      configuration.set(RemoteDataCollector.SEND_METRIC_ATTEMPTS, 1);
      RemoteDataCollector dataCollector = new RemoteDataCollector(
          new Configuration(),
          new MockManager(),
          new MockPipelineStoreTask(),
          new MockPipelineStateStore(),
          aclStoreTask,
          new RemoteStateEventListener(new Configuration()),
          runtimeInfo,
          buildInfo,
          Mockito.mock(AclCacheHelper.class),
          Mockito.mock(StageLibraryTask.class),
          Mockito.mock(BlobStoreTask.class),
          new SafeScheduledExecutorService(1, "supportBundleExecutor")
      );
      dataCollector = Mockito.spy(dataCollector);
      Mockito.when(dataCollector.getRemotePipelines()).thenReturn(new ArrayList<>());
      RemoteDataCollector.StopAndDeleteCallable stopAndDeleteCallable = new RemoteDataCollector.StopAndDeleteCallable(
          dataCollector,
          "user",
          "ns:name",
          "rev",
          600000,
          Mockito.mock(EventClient.class),
          "fakeUrl",
          new HashMap<>(),
          configuration
      );
      stopAndDeleteCallable.call();

      assertEquals(1, MockRunner.stopCalled);
      assertEquals(1, MockPipelineStoreTask.deleteCalled);
      assertEquals(1, MockPipelineStoreTask.deleteRulesCalled);
      Mockito.verify(dataCollector, Mockito.times(1)).getRemotePipelines();
    } finally {
      MockRunner.stopCalled = 0;
      MockPipelineStateStore.getStateCalled = 0;
      MockPipelineStoreTask.deleteCalled = 0;
      MockPipelineStoreTask.deleteRulesCalled = 0;
    }
  }

  @Test
  public void testGetPipelineStatus() throws Exception {
    try {
      RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
      mockDpmClientInfo(runtimeInfo);
      AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
      File testFolder = tempFolder.newFolder();
      Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());
      Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "ns:name", "rev").toPath());
      Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "ns:name1", "rev1").toPath());
      Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "ns:name2", "rev1").toPath());
      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");

      OffsetFileUtil.saveOffsets(runtimeInfo, "ns:name", "rev", Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:100"));
      OffsetFileUtil.saveOffsets(runtimeInfo, "ns:name1", "rev1", Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:101"));
      OffsetFileUtil.saveOffsets(runtimeInfo, "ns:name2", "rev1", Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:102"));

      RemoteDataCollector dataCollector = new RemoteDataCollector(
          new Configuration(),
          new MockManager(),
          new MockPipelineStoreTask(),
          new MockPipelineStateStore(),
          aclStoreTask,
          new RemoteStateEventListener(new Configuration()),
          runtimeInfo,
          buildInfo,
          Mockito.mock(AclCacheHelper.class),
          Mockito.mock(StageLibraryTask.class),
          Mockito.mock(BlobStoreTask.class),
          new SafeScheduledExecutorService(1, "supportBundleExecutor")
      );
      dataCollector.init();
      dataCollector.validateConfigs("user", "ns:name", "rev", Collections.emptyList());
      dataCollector.validateConfigs("user1", "ns:name1", "rev1", Collections.emptyList());
      Collection<PipelineAndValidationStatus> collectionPipelines = dataCollector.getPipelines();
      assertEquals(4, collectionPipelines.size());
      for (PipelineAndValidationStatus validationStatus : collectionPipelines) {
        assertTrue(validationStatus.getName().equals("ns:name") || validationStatus.getName().equals("ns:name1") ||
            validationStatus.getName().equals(
            "ns:name2") || validationStatus.getName().equals("local"));
        if (validationStatus.getName().equals("ns:name")) {
          assertEquals("title", validationStatus.getTitle());
          assertEquals("rev", validationStatus.getRev());
          assertEquals(PipelineStatus.EDITED, validationStatus.getPipelineStatus());
          assertEquals(ValidationStatus.VALID, validationStatus.getValidationStatus());
          assertNull(validationStatus.getMessage());
          assertNull(validationStatus.getIssues());
        } else if (validationStatus.getName().equals("ns:name1")) {
          assertEquals("rev1", validationStatus.getRev());
          assertEquals(PipelineStatus.EDITED, validationStatus.getPipelineStatus());
          assertEquals(ValidationStatus.INVALID, validationStatus.getValidationStatus());
          assertEquals(0, validationStatus.getIssues().getIssues().size());
        } else if (validationStatus.getName().equals("ns:name2")) {
          assertEquals("rev1", validationStatus.getRev());
          assertEquals(PipelineStatus.RUNNING, validationStatus.getPipelineStatus());
          assertNull(validationStatus.getValidationStatus());
          assertNull(validationStatus.getIssues());
        } else {
          assertEquals("rev1", validationStatus.getRev());
          assertEquals(PipelineStatus.RUNNING, validationStatus.getPipelineStatus());
          assertNull(validationStatus.getValidationStatus());
          assertNull(validationStatus.getIssues());
        }
      }
      assertTrue(dataCollector.getValidatorList().isEmpty());
    } finally {
      MockPreviewer.validateConfigsCalled = 0;
    }
  }

  @Test
  public void testAclOnSavePipeline() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
    RemoteDataCollector dataCollector = new RemoteDataCollector(
        new Configuration(),
        new MockManager(),
        new MockPipelineStoreTask(),
        new MockPipelineStateStore(),
        aclStoreTask,
        new RemoteStateEventListener(new Configuration()),
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "supportBundleExecutor")
    );
    File testFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());
    Acl acl = new Acl();
    SourceOffset sourceOffset = new SourceOffset();
    sourceOffset.setOffset("offset:1000");
    new SourceOffsetUpgrader().upgrade(sourceOffset);
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "foo", "0").toPath());
    dataCollector.savePipeline("user",
        "foo", "0",
        "",
        sourceOffset,
        mockPipelineConfiguration(),
        null,
        acl,
        new HashMap<String, Object>(),
        new HashMap<>()
    );
    Mockito.verify(aclStoreTask, Mockito.times(1)).saveAcl(Mockito.eq("foo"), Mockito.eq(acl));
  }

  @Test
  public void testSyncAcl() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
    PipelineStoreTask pipelineStoreTask = Mockito.mock(MockPipelineStoreTask.class);
    RemoteDataCollector dataCollector = new RemoteDataCollector(
        new Configuration(),
        new MockManager(),
        pipelineStoreTask,
        new MockPipelineStateStore(),
        aclStoreTask,
        new RemoteStateEventListener(new Configuration()),
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "supportBundleExecutor")
    );
    File testFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());
    Mockito.when(pipelineStoreTask.hasPipeline(Mockito.anyString())).thenReturn(true);
    Acl acl = new Acl();
    String name = "remote:pipeline";
    acl.setResourceId(name);
    dataCollector.syncAcl(acl);
    Mockito.verify(aclStoreTask, Mockito.times(1)).saveAcl(Mockito.eq(name), Mockito.eq(acl));
  }

  @Test
  public void testEncryptCredentialsEnabledOnSave() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
    PipelineStoreTask pipelineStoreTask = Mockito.spy(new MockPipelineStoreTask());
    RemoteDataCollector dataCollector = new RemoteDataCollector(
        new Configuration(),
        new MockManager(),
        pipelineStoreTask,
        new MockPipelineStateStore(),
        aclStoreTask,
        new RemoteStateEventListener(new Configuration()),
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "supportBundleExecutor")
    );
    File testFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "foo", "0").toPath());
    dataCollector.savePipeline("user",
        "foo",
        "0",
        "",
        new SourceOffset(),
        mockPipelineConfiguration(),
        null,
        new Acl(),
        Collections.emptyMap(),
        new HashMap<>()
    );
    Mockito.verify(pipelineStoreTask).save(
        Mockito.eq("user"),
        Mockito.eq("foo"),
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.any(PipelineConfiguration.class),
        Mockito.eq(true)
    );
  }

  @Test
  public void testSavePipelineOffset() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
    RemoteDataCollector dataCollector = new RemoteDataCollector(
        new Configuration(),
        new MockManager(),
        new MockPipelineStoreTask(),
        new MockPipelineStateStore(),
        aclStoreTask,
        new RemoteStateEventListener(new Configuration()),
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "supportBundleExecutor")
    );
    File testFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());
    SourceOffset sourceOffset = new SourceOffset();
    sourceOffset.setOffset("offset:1000");
    new SourceOffsetUpgrader().upgrade(sourceOffset);
    Files.createDirectories(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, "foo", "0").toPath());
    dataCollector.savePipeline("user",
        "foo", "0",
        "",
        sourceOffset,
        mockPipelineConfiguration(),
        null,
        new Acl(),
        new HashMap<String, Object>(),
        new HashMap<>()
    );
    assertTrue("Offset File doesn't exist", OffsetFileUtil.getPipelineOffsetFile(runtimeInfo, "foo", "0").exists());
    assertEquals(
        sourceOffset.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY),
        OffsetFileUtil.getOffsets(runtimeInfo, "foo", "0").get(Source.POLL_SOURCE_OFFSET_KEY)
    );
  }

  @Test
  public void testRemotePipelines() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
    RemoteStateEventListener remoteStateEventListener = Mockito.mock(RemoteStateEventListener.class);
    PipelineStoreTask pipelineStoreTask = Mockito.mock(MockPipelineStoreTask.class);
    RemoteDataCollector dataCollector = new RemoteDataCollector(
        new Configuration(),
        new MockManager(),
        pipelineStoreTask,
        new MockPipelineStateStore(),
        aclStoreTask,
        remoteStateEventListener,
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "supportBundleExecutor")
    );
    List<PipelineState> pipelineStates = new ArrayList<>();
    pipelineStates.add(new PipelineStateImpl("user",
        "name",
        "rev",
        PipelineStatus.RUNNING,
        "message",
        -1,
        new HashMap<String, Object>(),
        ExecutionMode.STANDALONE,
        "",
        -1,
        -1
    ));
    File testFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());
    Mockito.when(pipelineStoreTask.hasPipeline(Mockito.anyString())).thenReturn(false);
    Mockito.when(remoteStateEventListener.getPipelineStateEvents()).thenReturn(Arrays.<Pair<PipelineState,
        Map<String, String>>>asList(
        new ImmutablePair<>(pipelineStates.get(0), Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, "offset:1000"))));
    List<PipelineAndValidationStatus> pipelineAndValidationStatuses = dataCollector.getRemotePipelinesWithChanges();
    assertEquals(1, pipelineAndValidationStatuses.size());
    PipelineAndValidationStatus pipelineAndValidationStatus = pipelineAndValidationStatuses.get(0);
    assertEquals("name", pipelineAndValidationStatus.getName());
    assertNull(pipelineAndValidationStatus.getTitle());
    assertEquals("rev", pipelineAndValidationStatus.getRev());
    assertEquals(PipelineStatus.RUNNING, pipelineAndValidationStatus.getPipelineStatus());
    assertEquals(false, pipelineAndValidationStatus.isClusterMode());
    assertTrue(pipelineAndValidationStatus.getWorkerInfos().isEmpty());
    assertTrue(pipelineAndValidationStatus.isRemote());
    assertEquals("message", pipelineAndValidationStatus.getMessage());
    // "{\n" + "  \"offsets\" : {\n" + "    \"$com.streamsets.datacollector.pollsource.offset$\" : \"offset:1000\"\n" + "  },\n" + "  \"version\" : 2\n" + "}"

    SourceOffsetJson sourceOffsetJson = ObjectMapperFactory.get().readValue(
        pipelineAndValidationStatus.getOffset(),
        SourceOffsetJson.class
    );
    assertNotNull(sourceOffsetJson);
    assertEquals(2, sourceOffsetJson.getVersion());
    assertEquals("offset:1000", sourceOffsetJson.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
    assertNull(pipelineAndValidationStatus.getValidationStatus());
  }

  @Test
  public void testPipelineStateExists() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    Manager manager = Mockito.mock(StandaloneAndClusterPipelineManager.class);
    PipelineStoreTask pipelineStoreTask = Mockito.mock(PipelineStoreTask.class);
    PipelineStateStore pipelineStateStore = Mockito.mock(PipelineStateStore.class);
    Mockito.when(pipelineStateStore.getState("name", "rev")).thenThrow(new PipelineStoreException(ContainerError
        .CONTAINER_0209));
    RemoteDataCollector dataCollector = Mockito.spy(new RemoteDataCollector(
        new Configuration(),
        manager,
        pipelineStoreTask,
        pipelineStateStore,
        Mockito.mock(AclStoreTask.class),
        Mockito.mock(RemoteStateEventListener.class),
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "supportBundleExecutor")
    ));
    Assert.assertTrue(!dataCollector.pipelineStateExists("name", "rev"));
  }

  @Test
  public void testRunnerCount() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    try {
      Manager manager = Mockito.mock(StandaloneAndClusterPipelineManager.class);
      PipelineStoreTask pipelineStoreTask = Mockito.mock(PipelineStoreTask.class);
      RemoteDataCollector dataCollector = Mockito.spy(new RemoteDataCollector(
          new Configuration(),
          manager,
          pipelineStoreTask,
          Mockito.mock(PipelineStateStore.class),
          Mockito.mock(AclStoreTask.class),
          Mockito.mock(RemoteStateEventListener.class),
          runtimeInfo,
          buildInfo,
          Mockito.mock(AclCacheHelper.class),
          Mockito.mock(StageLibraryTask.class),
          Mockito.mock(BlobStoreTask.class),
          new SafeScheduledExecutorService(1, "supportBundleExecutor")
      ));
      PipelineState pipelineStatus1 = new PipelineStateImpl(
          "user",
          "ns:name",
          "rev",
          PipelineStatus.RUNNING,
          null,
          System.currentTimeMillis(),
          null,
          ExecutionMode.STANDALONE,
          null,
          0,
          -1
      );
      Mockito.when(manager.isPipelineActive(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
      Mockito.when(manager.getPipelines()).thenReturn(Arrays.asList(pipelineStatus1));
      Mockito.when(pipelineStoreTask.getInfo(Mockito.anyString())).thenReturn(Mockito.mock(PipelineInfo.class));
      Mockito.doReturn(pipelineStatus1.getPipelineId()).when(dataCollector).getSchGeneratedPipelineName(pipelineStatus1.getPipelineId(), pipelineStatus1.getRev());
      Runner runner = Mockito.mock(Runner.class);
      Mockito.when(runner.getRunnerCount()).thenReturn(10);
      Mockito.when(manager.getRunner(Mockito.anyString(), Mockito.anyString())).thenReturn(runner);
      assertEquals(1, dataCollector.getPipelines().size());
      PipelineAndValidationStatus pipelineAndValidationStatus = dataCollector.getPipelines().iterator().next();
      assertEquals(10, pipelineAndValidationStatus.getRunnerCount());
    } finally {
      MockPipelineStateStore.getStateCalled = 0;
    }
  }

  @Test
  public void testPreviewPipeline() throws Exception {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.17.0");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    mockDpmClientInfo(runtimeInfo);
    AclStoreTask aclStoreTask = Mockito.mock(AclStoreTask.class);
    final MockManager manager = new MockManager();
    RemoteDataCollector dataCollector = new RemoteDataCollector(
        new Configuration(),
        manager,
        new MockPipelineStoreTask(),
        new MockPipelineStateStore(),
        aclStoreTask,
        new RemoteStateEventListener(new Configuration()),
        runtimeInfo,
        buildInfo,
        Mockito.mock(AclCacheHelper.class),
        Mockito.mock(StageLibraryTask.class),
        Mockito.mock(BlobStoreTask.class),
        new SafeScheduledExecutorService(1, "testPreviewPipeline")
    );
    File testFolder = tempFolder.newFolder();
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testFolder.getAbsolutePath());

    final String previewerId = dataCollector.previewPipeline(
        "user",
        "ns:name",
        "rev",
        1,
        10,
        true,
        true,
        null,
        null,
        10000l,
        false,
        Collections.emptyList(),
        p -> {
          this.afterActionsFunctionParam = p;
          return null;
        },
        new HashMap<>()
    );
    final MockPreviewer lastPreviewer = manager.getLastPreviewer();

    assertThat(lastPreviewer, notNullValue());
    assertThat(lastPreviewer.getId(), equalTo(previewerId));

    lastPreviewer.stop();
    assertThat(this.afterActionsFunctionParam, equalTo(lastPreviewer));
    assertTrue(lastPreviewer.previewStopped);
  }

  private PipelineConfiguration mockPipelineConfiguration() {
    PipelineInfo info = Mockito.mock(PipelineInfo.class);
    Mockito.when(info.getSdcVersion()).thenReturn("3.17.0");
    PipelineConfiguration pipelineConfiguration = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pipelineConfiguration.getInfo()).thenReturn(info);

    return pipelineConfiguration;
  }
}
