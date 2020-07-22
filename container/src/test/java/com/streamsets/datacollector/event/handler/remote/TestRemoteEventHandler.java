/* Copyright 2017 StreamSets Inc.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.json.PipelineConfigAndRulesJson;
import com.streamsets.datacollector.config.json.PipelineStatusJson;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.api.EventException;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PipelinePreviewEvent;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.dto.SDCBuildInfo;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.StageInfo;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask.EventHandlerCallable;
import com.streamsets.datacollector.event.json.BlobDeleteVersionEventJson;
import com.streamsets.datacollector.event.json.BlobStoreEventJson;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.DisconnectedSsoCredentialsEventJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineDeleteEventJson;
import com.streamsets.datacollector.event.json.PipelineHistoryDeleteEventJson;
import com.streamsets.datacollector.event.json.PipelineResetEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStartEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventsJson;
import com.streamsets.datacollector.event.json.PipelineStopAndDeleteEventJson;
import com.streamsets.datacollector.event.json.PipelineStopEventJson;
import com.streamsets.datacollector.event.json.PipelineValidateEventJson;
import com.streamsets.datacollector.event.json.SDCInfoEventJson;
import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.event.json.SaveConfigurationEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.datacollector.event.json.StageInfoJson;
import com.streamsets.datacollector.event.json.SyncAclEventJson;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.execution.manager.PipelineStateImpl;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.ProductBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.json.AclJson;
import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.notNullValue;

public class TestRemoteEventHandler {

  private static final UUID id1 = UUID.randomUUID();
  private static final UUID id2 = UUID.randomUUID();
  private static final UUID id3 = UUID.randomUUID();
  private static final UUID id4 = UUID.randomUUID();
  private static final UUID id5 = UUID.randomUUID();
  private static final UUID id6 = UUID.randomUUID();
  private static final UUID id7 = UUID.randomUUID();
  private static final UUID id8 = UUID.randomUUID();
  private static final UUID id9 = UUID.randomUUID();
  private static final UUID id10 = UUID.randomUUID();
  private static final UUID id11 = UUID.randomUUID();

  private static final long PING_FREQUENCY = 10;
  private static final MessagingJsonToFromDto jsonDto = MessagingJsonToFromDto.INSTANCE;

  private final BuildInfo buildInfo = ProductBuildInfo.getDefault();

  private static class MockBaseEventSenderReceiver implements EventClient {
    public List<ClientEventJson> clientJson;

    @Override
    public List<ServerEventJson> submit(
        String path,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        boolean compression,
        List<ClientEventJson> clientEventJson
    ) throws EventException {
      this.clientJson = clientEventJson;
      final String name = "name";
      final String rev = "rev";
      final String user = "user";

      BlobStoreEventJson blobStoreEvent = new BlobStoreEventJson();
      blobStoreEvent.setNamespace("n");
      blobStoreEvent.setVersion(1l);
      blobStoreEvent.setId("a");
      blobStoreEvent.setContent("X");
      BlobDeleteVersionEventJson blobDeleteEvent = new BlobDeleteVersionEventJson();
      blobDeleteEvent.setNamespace("n");
      blobDeleteEvent.setVersion(1l);
      blobDeleteEvent.setId("a");
      SaveConfigurationEventJson saveConfigurationEvent = new SaveConfigurationEventJson();
      saveConfigurationEvent.setConfiguration(Collections.singletonMap("a", "b"));

      List<ServerEventJson> serverEventJsonList = new ArrayList<ServerEventJson>();
      try {
        ServerEventJson serverEventJson1 = new ServerEventJson();
        ServerEventJson serverEventJson2 = new ServerEventJson();
        ServerEventJson serverEventJson3 = new ServerEventJson();
        ServerEventJson serverEventJson4 = new ServerEventJson();
        ServerEventJson serverEventJson5 = new ServerEventJson();
        ServerEventJson serverEventJson6 = new ServerEventJson();
        ServerEventJson serverEventJson7 = new ServerEventJson();
        ServerEventJson serverEventJson8 = new ServerEventJson();
        ServerEventJson serverEventJson9 = new ServerEventJson();
        ServerEventJson serverEventJson10 = new ServerEventJson();
        ServerEventJson serverEventJson11 = new ServerEventJson();
        setServerEvent(
            serverEventJson1,
            id1.toString(),
            EventType.START_PIPELINE,
            false,
            true,
            jsonDto.serialize(createStartEvent(name, rev, user))
        );
        setServerEvent(
            serverEventJson2,
            id2.toString(),
            EventType.STOP_PIPELINE,
            false,
            true,
            jsonDto.serialize(createStopEvent(name, rev, user))
        );
        setServerEvent(
            serverEventJson3,
            id3.toString(),
            EventType.DELETE_PIPELINE,
            false,
            true,
            jsonDto.serialize(createDeleteEvent(name, rev, user))
        );
        setServerEvent(serverEventJson4,
            id4.toString(),
            EventType.DELETE_HISTORY_PIPELINE,
            false,
            true,
            jsonDto.serialize(createHistoryDeleteEvent(name, rev, user))
        );
        setServerEvent(
            serverEventJson5,
            id5.toString(),
            EventType.VALIDATE_PIPELINE,
            false,
            true,
            jsonDto.serialize(createValidateEvent(name, rev, user))
        );
        setServerEvent(serverEventJson6,
            id6.toString(),
            EventType.RESET_OFFSET_PIPELINE,
            false,
            true,
            jsonDto.serialize(createResetEvent(name, rev, user))
        );
        setServerEvent(
            serverEventJson7,
            id7.toString(),
            EventType.STOP_DELETE_PIPELINE,
            false,
            true,
            jsonDto.serialize(createStopAndDeleteEvent(name, rev, user))
        );
        setServerEvent(
            serverEventJson9,
            id9.toString(),
            EventType.BLOB_STORE,
            false,
            true,
            jsonDto.serialize(blobStoreEvent)
        );
        setServerEvent(
            serverEventJson10,
            id10.toString(),
            EventType.BLOB_DELETE_VERSION,
            false,
            true,
            jsonDto.serialize(blobDeleteEvent)
        );
        setServerEvent(
            serverEventJson11,
            id11.toString(),
            EventType.SAVE_CONFIGURATION,
            false,
            false,
            jsonDto.serialize(saveConfigurationEvent)
        );
        SyncAclEventJson syncAclEventJson = new SyncAclEventJson();
        AclJson aclJson = new AclJson();
        aclJson.setResourceId("remote:name");
        syncAclEventJson.setAcl(aclJson);
        setServerEvent(
            serverEventJson8,
            id8.toString(),
            EventType.SYNC_ACL,
            false,
            false,
            jsonDto.serialize(syncAclEventJson)
        );

        serverEventJsonList.addAll(Arrays.asList(serverEventJson1,
            serverEventJson2,
            serverEventJson3,
            serverEventJson4,
            serverEventJson5,
            serverEventJson6,
            serverEventJson7,
            serverEventJson8,
            serverEventJson9,
            serverEventJson10,
            serverEventJson11
        ));

      } catch (JsonProcessingException e) {
        throw new EventException("Cannot create event for test case" + e.getMessage());
      }
      return serverEventJsonList;
    }

    @Override
    public void submit(
        String path,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        List<SDCMetricsJson> sdcMetricsJsons,
        long retryAttempts
    ) {

    }

    @Override
    public void sendSyncEvents(
        String absoluteTargetUrl,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        Event event,
        long retryAttempts
    ) {

    }
  }

  private static PipelineStartEventJson createStartEvent(String name, String rev, String user) {
    PipelineStartEventJson pipelineStartEventJson = setBaseEventPropertiesAndReturn(new PipelineStartEventJson(), name, rev, user);
    pipelineStartEventJson.setGroups(Arrays.asList("all"));
    return pipelineStartEventJson;
  }

  private static PipelineStopEventJson createStopEvent(String name, String rev, String user) {
    final PipelineStopEventJson stopEventJson = new PipelineStopEventJson();
    stopEventJson.setName(name);
    stopEventJson.setRev(rev);
    stopEventJson.setUser(user);
    return stopEventJson;
  }

  private static PipelineDeleteEventJson createDeleteEvent(String name, String rev, String user) {
    final PipelineDeleteEventJson deleteEventJson = new PipelineDeleteEventJson();
    deleteEventJson.setName(name);
    deleteEventJson.setRev(rev);
    deleteEventJson.setUser(user);
    return deleteEventJson;
  }

  private static PipelineHistoryDeleteEventJson createHistoryDeleteEvent(String name, String rev, String user) {
    final PipelineHistoryDeleteEventJson deleteHistoryEventJson = new PipelineHistoryDeleteEventJson();
    deleteHistoryEventJson.setName(name);
    deleteHistoryEventJson.setRev(rev);
    deleteHistoryEventJson.setUser(user);
    return deleteHistoryEventJson;
  }

  private static PipelineValidateEventJson createValidateEvent(String name, String rev, String user) {
    final PipelineValidateEventJson validateEventJson = new PipelineValidateEventJson();
    validateEventJson.setName(name);
    validateEventJson.setRev(rev);
    validateEventJson.setUser(user);
    return validateEventJson;
  }

  private static PipelineResetEventJson createResetEvent(String name, String rev, String user) {
    final PipelineResetEventJson resetEventJson = new PipelineResetEventJson();
    resetEventJson.setName(name);
    resetEventJson.setRev(rev);
    resetEventJson.setUser(user);
    return resetEventJson;
  }

  private static PipelineStopAndDeleteEventJson createStopAndDeleteEvent(String name, String rev, String user) {
    final PipelineStopAndDeleteEventJson stopAndDeleteEventJson = new PipelineStopAndDeleteEventJson();
    stopAndDeleteEventJson.setName(name);
    stopAndDeleteEventJson.setRev(rev);
    stopAndDeleteEventJson.setUser(user);
    return stopAndDeleteEventJson;
  }

  private static <E extends PipelineBaseEventJson> E setBaseEventPropertiesAndReturn(
      E baseEvent,
      String name,
      String rev,
      String user
  ) {
    baseEvent.setName(name);
    baseEvent.setRev(rev);
    baseEvent.setUser(user);
    return baseEvent;
  }

  private static void setServerEvent(
      ServerEventJson serverEventJson,
      String eventId,
      EventType eventType,
      boolean isAckEvent,
      boolean requiresAck,
      String payload
  ) {
    serverEventJson.setAckEvent(isAckEvent);
    serverEventJson.setEventId(eventId);
    serverEventJson.setEventTypeId(eventType.getValue());
    serverEventJson.setFrom("JOB_RUNNER");
    serverEventJson.setRequiresAck(requiresAck);
    serverEventJson.setPayload(payload);
  }

  private static class MockPingFrequencyAdjustmentSenderReceiver implements EventClient {
    @Override
    public List<ServerEventJson> submit(
        String path,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        boolean compression,
        List<ClientEventJson> clientEventJson
    ) throws EventException {
      PingFrequencyAdjustmentEventJson pingFrequencyJson = new PingFrequencyAdjustmentEventJson();
      pingFrequencyJson.setPingFrequency(PING_FREQUENCY);
      ServerEventJson serverEventJson1 = new ServerEventJson();
      try {
        setServerEvent(
            serverEventJson1,
            id1.toString(),
            EventType.PING_FREQUENCY_ADJUSTMENT,
            false,
            true,
            jsonDto.serialize(pingFrequencyJson)
        );
      } catch (Exception e) {
        throw new EventException(e.getMessage());
      }
      return Arrays.asList(serverEventJson1);
    }

    @Override
    public void submit(
        String path,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        List<SDCMetricsJson> sdcMetricsJsons,
        long retryAttempts
    ) {

    }

    @Override
    public void sendSyncEvents(
        String absoluteTargetUrl,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        Event event,
        long retryAttempts
    ) {

    }
  }

  private static class MockSaveEventSenderReceiver implements EventClient {
    @Override
    public List<ServerEventJson> submit(
        String path,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        boolean compression,
        List<ClientEventJson> clientEventJson
    ) throws EventException {
      try {
        List<ServerEventJson> serverEventJsonList = new ArrayList<ServerEventJson>();
        PipelineSaveEventJson pipelineSaveEventJson = new PipelineSaveEventJson();
        pipelineSaveEventJson.setName("name");
        pipelineSaveEventJson.setRev("rev");
        pipelineSaveEventJson.setUser("user");
        pipelineSaveEventJson.setDescription("description");
        List<Config> list = new ArrayList<Config>();
        list.add(new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()));
        Map<String, Object> uiInfo = new HashMap<String, Object>();
        uiInfo.put("uiInfo1", 123);
        // API Config needs fixing
        PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
            PipelineConfigBean.VERSION,
            "pipelineId",
            UUID.randomUUID(),
            "label",
            "description",
            list,
            uiInfo,
            MockStages.getSourceStageConfig(),
            MockStages.getErrorStageConfig(),
            null,
            Collections.emptyList(),
            Collections.emptyList()
        );
        PipelineConfigurationJson pipelineConfigJson = BeanHelper.wrapPipelineConfiguration(pipelineConf);
        PipelineConfigAndRulesJson configRulesJson = new PipelineConfigAndRulesJson();
        configRulesJson.setPipelineConfig(MessagingJsonToFromDto.INSTANCE.serialize(pipelineConfigJson));
        List<MetricsRuleDefinition> metricRulesList = new ArrayList<MetricsRuleDefinition>();
        ;
        metricRulesList.add(new MetricsRuleDefinition("id",
            "alertText",
            "metricId",
            MetricType.GAUGE,
            MetricElement.COUNTER_COUNT,
            "condition",
            false,
            true,
            System.currentTimeMillis()
        ));
        RuleDefinitions ruleDefinitions = new RuleDefinitions(
            PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
            RuleDefinitionsConfigBean.VERSION,
            metricRulesList,
            new ArrayList<DataRuleDefinition>(),
            new ArrayList<DriftRuleDefinition>(),
            new ArrayList<String>(),
            id1,
            Collections.emptyList()
        );
        configRulesJson.setPipelineRules(MessagingJsonToFromDto.INSTANCE.serialize(BeanHelper.wrapRuleDefinitions(
            ruleDefinitions)));
        pipelineSaveEventJson.setPipelineConfigurationAndRules(configRulesJson);
        PipelineSaveRulesEventJson pipelineSaveRulesEventJson = new PipelineSaveRulesEventJson();
        pipelineSaveRulesEventJson.setName("name");
        pipelineSaveRulesEventJson.setRev("rev");
        pipelineSaveRulesEventJson.setUser("user");
        pipelineSaveRulesEventJson.setRuleDefinitions(MessagingJsonToFromDto.INSTANCE.serialize(BeanHelper
            .wrapRuleDefinitions(
            ruleDefinitions)));
        ServerEventJson serverEventJson1 = new ServerEventJson();
        ServerEventJson serverEventJson2 = new ServerEventJson();
        ServerEventJson serverEventJson3 = new ServerEventJson();
        setServerEvent(
            serverEventJson1,
            id1.toString(),
            EventType.SAVE_PIPELINE,
            false,
            true,
            jsonDto.serialize(pipelineSaveEventJson)
        );
        setServerEvent(
            serverEventJson2,
            id2.toString(),
            EventType.SAVE_RULES_PIPELINE,
            false,
            true,
            jsonDto.serialize(pipelineSaveRulesEventJson)
        );
        setServerEvent(
            serverEventJson3,
            id3.toString(),
            EventType.SSO_DISCONNECTED_MODE_CREDENTIALS,
            false,
            false,
            jsonDto.serialize(new DisconnectedSsoCredentialsEventJson())
        );
        serverEventJsonList.addAll(Arrays.asList(serverEventJson1, serverEventJson2, serverEventJson3));
        return serverEventJsonList;

      } catch (Exception e) {
        throw new EventException("Cannot create event for test case " + e.getMessage());
      }
    }

    @Override
    public void submit(
        String path,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        List<SDCMetricsJson> sdcMetricsJsons,
        long retryAttempts
    ) {

    }

    @Override
    public void sendSyncEvents(
        String absoluteTargetUrl,
        Map<String, String> queryParams,
        Map<String, String> headerParams,
        Event event,
        long retryAttempts
    ) {

    }

  }

  private static class MockRemoteDataCollector implements DataCollector {

    public int startCalled;
    public boolean stopCalled;
    public boolean deleteCalled;
    public boolean deleteHistoryCalled;
    public boolean savePipelineCalled;
    public boolean savePipelineRulesCalled;
    public boolean resetOffsetCalled;
    public boolean validateConfigsCalled;
    public boolean getPipelinesCalled;
    public boolean errorInjection;
    public boolean putDummyPipelineStatus;
    public boolean stopDeletePipelineCalled;
    public boolean putRemotePipelines;
    public boolean syncAclCalled;
    public boolean blobStoreCalled;
    public boolean blobDeleteCalled;
    public Map<String, String> savedConfiguration;

    @Override
    public void init() {
      // no-op
    }

    @Override
    public void start(Runner.StartPipelineContext context, String name, String rev, Set<String> groups) throws PipelineException, StageException {
      startCalled++;
      if (errorInjection) {
        throw new PipelineException(ContainerError.CONTAINER_0001);
      }
    }

    @Override
    public void stop(
        String user,
        String name,
        String rev
    ) throws PipelineStoreException, PipelineManagerException, PipelineException {
      stopCalled = true;
    }

    @Override
    public void delete(String name, String rev) throws PipelineStoreException {
      deleteCalled = true;
    }

    @Override
    public void deleteHistory(
        String user,
        String name,
        String rev
    ) throws PipelineStoreException, PipelineManagerException {
      deleteHistoryCalled = true;
    }

    @Override
    public String savePipeline(
        String user,
        String name, String rev,
        String description,
        SourceOffset offset,
        PipelineConfiguration pipelineConfiguration,
        RuleDefinitions ruleDefinitions, Acl acl, Map<String, Object> metadata,
        Map<String, ConnectionConfiguration> connections
    ) throws PipelineStoreException {
      savePipelineCalled = true;
      return "";
    }

    @Override
    public void savePipelineRules(
        String name,
        String rev,
        RuleDefinitions ruleDefinitions
    ) throws PipelineStoreException {
      savePipelineRulesCalled = true;
    }

    @Override
    public void resetOffset(
        String user,
        String name,
        String rev
    ) throws PipelineStoreException, PipelineRunnerException, PipelineManagerException {
      resetOffsetCalled = true;
    }

    @Override
    public void validateConfigs(
        String user,
        String name,
        String rev,
        List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs
    ) throws PipelineException {
      validateConfigsCalled = true;
    }

    @Override
    public String previewPipeline(
        String user,
        String name,
        String rev,
        int batches,
        int batchSize,
        boolean skipTargets,
        boolean skipLifecycleEvents,
        String stopStage,
        List<StageOutput> stagesOverride,
        long timeoutMillis,
        boolean testOrigin,
        List<PipelineStartEvent.InterceptorConfiguration> interceptorConfs,
        Function<Object, Void> afterActionsFunction,
        Map<String, ConnectionConfiguration> connections
    ) throws PipelineException {
      // no-op for now
      return null;
    }

    @Override
    public Collection<PipelineAndValidationStatus> getPipelines() throws PipelineStoreException {
      getPipelinesCalled = true;
      List<PipelineAndValidationStatus> list = new ArrayList<PipelineAndValidationStatus>();
      if (putDummyPipelineStatus) {
        list.add(new PipelineAndValidationStatus(
            "name1",
            "title1",
            "rev1",
            -1,
            false,
            PipelineStatus.RUNNING,
            "message",
            null,
            false,
            null,
            null,
            10
        ));
        list.add(new PipelineAndValidationStatus(
            "name2",
            "title2",
            "rev2",
            -1,
            false,
            PipelineStatus.CONNECTING,
            "message",
            null,
            false,
            null,
            null,
            10
        ));
      }
      return list;
    }

    @Override
    public List<PipelineAndValidationStatus> getRemotePipelinesWithChanges() throws PipelineException {
      List<PipelineAndValidationStatus> list = new ArrayList<>();
      if (putRemotePipelines) {
        list.add(new PipelineAndValidationStatus(
            "remote",
            "title",
            "rev1",
            -1,
            true,
            PipelineStatus.RUNNING,
            "message",
            null,
            false,
            null,
            null,
            10
        ));
      }
      return list;
    }

    @Override
    public void syncAcl(Acl acl) throws PipelineException {
      syncAclCalled = true;
    }

    @Override
    public void blobStore(String namespace, String id, long version, String content) throws StageException {
      this.blobStoreCalled = true;
    }

    @Override
    public void blobDelete(String namespace, String id) throws StageException {
      this.blobStoreCalled = true;
    }

    @Override
    public void blobDelete(String namespace, String id, long version) throws StageException {
      this.blobDeleteCalled = true;
    }

    @Override
    public void storeConfiguration(Map<String, String> newConfiguration) throws IOException {
      this.savedConfiguration = newConfiguration;
    }

    @Override
    public Future<AckEvent> stopAndDelete(String user, String name, String rev, long forceStopTimeout) throws PipelineException,
        StageException {
      stopDeletePipelineCalled = true;
      return null;
    }

    @Override
    public Runner getRunner(String name, String rev) throws PipelineException {
      return new MockMetricsRunner();
    }

    @Override
    public List<PipelineState> getRemotePipelines() throws PipelineException {
      return new ArrayList<>();
    }
  }

  public static class MockMetricsRunner extends TestRemoteDataCollector.MockRunner {

    @Override
    public PipelineConfiguration getPipelineConfiguration(String user) throws PipelineException {
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
    public Object getMetrics() {
      // TODO Auto-generated method stub
      MetricRegistry metricRegistry = new MetricRegistry();
      Counter counter = metricRegistry.counter("batchInputRecords");
      counter.inc(100);
      return metricRegistry;
    }
  }

  @Test
  public void testPipelineBaseEventTriggered() throws Exception {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    final MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector, new SafeScheduledExecutorService(1, "testPipelineBaseEventTriggered"),
        new SafeScheduledExecutorService(1, "syncSender"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(mockRemoteDataCollector,
        eventSenderReceiver,
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<String, String>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(8, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    assertTrue(ackEventList.get(0).getEvent() instanceof AckEvent);
    AckEvent ackEvent = (AckEvent) ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id2.toString(), ackEventList.get(1).getEventId());
    assertTrue(ackEventList.get(1).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent) ackEventList.get(1).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id3.toString(), ackEventList.get(2).getEventId());
    assertTrue(ackEventList.get(2).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent) ackEventList.get(2).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id4.toString(), ackEventList.get(3).getEventId());
    assertTrue(ackEventList.get(3).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent) ackEventList.get(3).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id5.toString(), ackEventList.get(4).getEventId());
    assertTrue(ackEventList.get(4).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent) ackEventList.get(4).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id6.toString(), ackEventList.get(5).getEventId());
    assertTrue(ackEventList.get(5).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent) ackEventList.get(5).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(1, mockRemoteDataCollector.startCalled);
    assertTrue(mockRemoteDataCollector.stopCalled);
    assertTrue(mockRemoteDataCollector.stopDeletePipelineCalled);
    assertTrue(mockRemoteDataCollector.resetOffsetCalled);
    assertTrue(mockRemoteDataCollector.validateConfigsCalled);
    assertTrue(mockRemoteDataCollector.deleteCalled);
    assertTrue(mockRemoteDataCollector.deleteHistoryCalled);
    assertTrue(mockRemoteDataCollector.getPipelinesCalled);
    assertTrue(mockRemoteDataCollector.syncAclCalled);
    assertTrue(mockRemoteDataCollector.blobStoreCalled);
    assertTrue(mockRemoteDataCollector.blobDeleteCalled);
    assertFalse(mockRemoteDataCollector.savePipelineCalled);
    assertFalse(mockRemoteDataCollector.savePipelineRulesCalled);

    assertNotNull(mockRemoteDataCollector.savedConfiguration);
    assertTrue(mockRemoteDataCollector.savedConfiguration.containsKey("a"));
    assertEquals("b", mockRemoteDataCollector.savedConfiguration.get("a"));
  }

  @Test
  public void testPipelineSaveEventTriggered() throws Exception {
    DataStore dataStore = Mockito.mock(DataStore.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Mockito.when(dataStore.getOutputStream()).thenReturn(baos);
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    final MockSaveEventSenderReceiver eventSenderReceiver = new MockSaveEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector, new SafeScheduledExecutorService(1, "testPipelineSaveEventTriggered"),
        new SafeScheduledExecutorService(1, "testPipelineStartEventTriggered"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration(),
        dataStore
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        eventSenderReceiver,
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<String, String>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(2, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    AckEvent ackEvent = (AckEvent) ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());
    assertTrue(mockRemoteDataCollector.savePipelineCalled);
    assertTrue(mockRemoteDataCollector.savePipelineRulesCalled);
  }

  @Test
  public void testPipelineAckEventError() {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1, "testPipelineAckEventError"),
        new SafeScheduledExecutorService(1, "testPipelineAckEventError"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        eventSenderReceiver,
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<String, String>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    // start event in error
    mockRemoteDataCollector.errorInjection = true;
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(8, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    assertTrue(ackEventList.get(0).getEvent() instanceof AckEvent);
    AckEvent ackEvent = (AckEvent) ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.ERROR, ackEvent.getAckEventStatus());
  }

  @Test
  public void testPingFrequencyEvent() {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1,"testPingFrequencyEvent"),
        new SafeScheduledExecutorService(1, "testPingFrequencyEvent"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        new MockPingFrequencyAdjustmentSenderReceiver(),
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    assertEquals(PING_FREQUENCY, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(1, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    assertTrue(ackEventList.get(0).getEvent() instanceof AckEvent);
    AckEvent ackEvent = (AckEvent) ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());
  }


  @Test
  public void testSendingEventClientToServer() throws Exception {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    mockRemoteDataCollector.putDummyPipelineStatus = true;
    mockRemoteDataCollector.putRemotePipelines = true;
    MockBaseEventSenderReceiver mockBaseEventSenderReceiver = new MockBaseEventSenderReceiver();
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    stopwatch.start();
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1, "testSendingEventClientToServer"),
        new SafeScheduledExecutorService(1, "testSendingEventClientToServer"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        mockBaseEventSenderReceiver,
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<>(),
        stopwatch,
        60000,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    assertEquals(1, mockBaseEventSenderReceiver.clientJson.size());
    ClientEventJson clientEventJson = mockBaseEventSenderReceiver.clientJson.get(0);
    PipelineStatusEventJson pipelineStatusEventJson = jsonToFromDto.deserialize(
        clientEventJson.getPayload(),
        new TypeReference<PipelineStatusEventJson>() {
        }
    );
    assertEquals("remote", pipelineStatusEventJson.getName());
    mockRemoteDataCollector.putRemotePipelines = false;
    Thread.sleep(10);
    remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        mockBaseEventSenderReceiver,
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<>(),
        stopwatch,
        5,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    assertEquals(2, mockBaseEventSenderReceiver.clientJson.size());
    clientEventJson = mockBaseEventSenderReceiver.clientJson.get(0);
    PipelineStatusEventsJson pipelineStatusEventsJson = jsonToFromDto.deserialize(
        clientEventJson.getPayload(),
        new TypeReference<PipelineStatusEventsJson>() {
        }
    );
    List<PipelineStatusEventJson> pipelineStateInfoList = pipelineStatusEventsJson.getPipelineStatusEventList();
    assertEquals("name1", pipelineStateInfoList.get(0).getName());
    assertEquals("title1", pipelineStateInfoList.get(0).getTitle());
    assertEquals("rev1", pipelineStateInfoList.get(0).getRev());
    assertEquals(PipelineStatusJson.RUNNING, pipelineStateInfoList.get(0).getPipelineStatus());
    assertEquals(10, pipelineStateInfoList.get(0).getRunnerCount());

    assertEquals("name2", pipelineStateInfoList.get(1).getName());
    assertEquals("title2", pipelineStateInfoList.get(1).getTitle());
    assertEquals("rev2", pipelineStateInfoList.get(1).getRev());
    assertEquals(PipelineStatusJson.CONNECTING, pipelineStateInfoList.get(1).getPipelineStatus());
  }

  @Test
  public void testSendSDCInfoEvent() throws Exception {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    MockBaseEventSenderReceiver mockBaseEventSenderReceiver = new MockBaseEventSenderReceiver();
    StageInfo stageInfo = new StageInfo("stage1", 1, "stageLib");
    List<StageInfo> stageInfoList = new ArrayList<>();
    stageInfoList.add(stageInfo);
    SDCBuildInfo sdcBuildInfo = new SDCBuildInfo("1.0", "date1", "foo", "sha1", "checksum1");
    SDCInfoEvent sdcInfoEvent = new SDCInfoEvent(
        "1",
        "localhost:9090",
        "1.7",
        stageInfoList,
        sdcBuildInfo,
        Arrays.asList("label_1", "label_2"),
        2,
        "foo",
        10000
    );
    ClientEvent clientEvent = new ClientEvent(
        id1.toString(),
        Arrays.asList("JOB_RUNNER"),
        false,
        false,
        EventType.SDC_INFO_EVENT,
        sdcInfoEvent,
        null
    );
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1, "testSendSDCInfoEvent"),
        new SafeScheduledExecutorService(1, "testSendSDCInfoEvent"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        mockBaseEventSenderReceiver,
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        clientEvent,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    assertEquals(3, mockBaseEventSenderReceiver.clientJson.size());
    assertEquals(EventType.SDC_INFO_EVENT.getValue(), mockBaseEventSenderReceiver.clientJson.get(0).getEventTypeId());
    assertEquals(
        EventType.STATUS_MULTIPLE_PIPELINES.getValue(),
        mockBaseEventSenderReceiver.clientJson.get(1).getEventTypeId()
    );
    assertEquals(Arrays.asList("JOB_RUNNER"), mockBaseEventSenderReceiver.clientJson.get(0).getDestinations());
    assertEquals(id1.toString(), mockBaseEventSenderReceiver.clientJson.get(0).getEventId());
    String payload = mockBaseEventSenderReceiver.clientJson.get(0).getPayload();
    SDCInfoEventJson sdcInfoJson = jsonToFromDto.deserialize(payload, new TypeReference<SDCInfoEventJson>() {
    });
    assertEquals("1", sdcInfoJson.getSdcId());
    assertEquals("localhost:9090", sdcInfoJson.getHttpUrl());
    assertEquals("1.7", sdcInfoJson.getJavaVersion());
    List<StageInfoJson> stageInfoListJson = sdcInfoJson.getStageDefinitionList();
    assertEquals(1, stageInfoListJson.size());
    assertEquals("stage1", stageInfoListJson.get(0).getStageName());
    assertEquals(1, stageInfoListJson.get(0).getStageVersion());
    assertEquals("stageLib", stageInfoListJson.get(0).getLibraryName());
    assertEquals(Arrays.asList("label_1", "label_2"), sdcInfoJson.getLabels());
    payload = mockBaseEventSenderReceiver.clientJson.get(1).getPayload();
    PipelineStatusEventsJson statusEventJson = jsonToFromDto.deserialize(payload,
        new TypeReference<PipelineStatusEventsJson>() {
        }
    );
    assertTrue(statusEventJson.getPipelineStatusEventList().isEmpty());
  }

  @Test
  public void testDisconnectedSsoCredentialsEvent() throws IOException {
    DataStore dataStore = Mockito.mock(DataStore.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Mockito.when(dataStore.getOutputStream()).thenReturn(baos);
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1, "testDisconnectedSsoCredentialsEvent"),
        new SafeScheduledExecutorService(1, "testDisconnectedSsoCredentialsEvent"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration(),
        dataStore
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        new MockSaveEventSenderReceiver(),
        jsonToFromDto,
        ackEventJsonList,
        new ArrayList<ClientEvent>(),
        null,
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    remoteEventHandler.callRemoteControl();
    Mockito.verify(dataStore, Mockito.times(1)).commit(Mockito.any(OutputStream.class));
    Assert.assertEquals(
        ImmutableMap.of("entries", Collections.emptyList()),
        new ObjectMapper().readValue(baos.toByteArray(), Map.class)
    );
  }

  @Test
  public void testAckIgnore() {
    final RemoteDataCollector mockRemoteDataCollector = Mockito.mock(RemoteDataCollector.class);
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1, "testDisconnectedSsoCredentialsEvent"),
        new SafeScheduledExecutorService(1, "testDisconnectedSsoCredentialsEvent"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );
    EventHandlerCallable remoteEventHandler = remoteEventHandlerTask.new EventHandlerCallable(
        mockRemoteDataCollector,
        Mockito.mock(EventClient.class),
        MessagingJsonToFromDto.INSTANCE,
        new ArrayList<>(),
        new ArrayList<>(),
        Mockito.mock(ClientEvent.class),
        null,
        -1,
        Arrays.asList("JOB_RUNNER"),
        ImmutableList.of("jobrunner-app", "timeseries-app"),
        new HashMap<>(),
        Stopwatch.createStarted(),
        -1,
        -1,
        new HashMap<>(),
        mockRuntimeInfo
    );
    ServerEventJson serverEventJson = new ServerEventJson();
    serverEventJson.setRequiresAck(true);
    serverEventJson.setEventTypeId(100000);
    ClientEvent clientEvent = remoteEventHandler.handlePipelineEvent(serverEventJson);
    Assert.assertNotNull(clientEvent);
    Assert.assertEquals(EventType.ACK_EVENT, clientEvent.getEventType());
    Assert.assertEquals(AckEventStatus.IGNORE, ((AckEvent)clientEvent.getEvent()).getAckEventStatus());
    serverEventJson.setRequiresAck(false);
    clientEvent = remoteEventHandler.handlePipelineEvent(serverEventJson);
    Assert.assertNull(clientEvent);
  }

  @Test
  public void testPreviewEventLocal() throws PipelineException {
    final RemoteDataCollector mockRemoteDataCollector = Mockito.mock(RemoteDataCollector.class);
    final String previewerId = "previewerId";
    Mockito.when(mockRemoteDataCollector.previewPipeline(
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.anyBoolean(),
        Mockito.anyBoolean(),
        Mockito.anyString(),
        Mockito.anyList(),
        Mockito.anyLong(),
        Mockito.anyBoolean(),
        Mockito.anyList(),
        Mockito.any(),
        Mockito.anyMap()
    )).thenReturn(previewerId);
    final MockBaseEventSenderReceiver eventSenderReceiver = new MockBaseEventSenderReceiver();
    final StageLibraryTask mockStageLibraryTask = new MockStages.MockStageLibraryTask.Builder().build();
    final RuntimeInfo mockRuntimeInfo = Mockito.mock(RuntimeInfo.class);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        mockRemoteDataCollector,
        new SafeScheduledExecutorService(1, "testDisconnectedSsoCredentialsEvent"),
        new SafeScheduledExecutorService(1, "testDisconnectedSsoCredentialsEvent"),
        mockStageLibraryTask,
        buildInfo,
        mockRuntimeInfo,
        new Configuration()
    );

    final RemoteDataCollectorResult result = remoteEventHandlerTask.handleLocalEvent(new PipelinePreviewEvent(
        "name",
        "rev",
        "user",
        Collections.emptyList(),
        1,
        10,
        false,
        false,
        null,
        1000l,
        false,
        null,
        Arrays.asList("all")
    ), EventType.PREVIEW_PIPELINE, new HashMap<>());

    assertThat(result.isError(), equalTo(false));
    assertThat(result.getImmediateResult(), notNullValue());
    assertThat(result.getImmediateResult(), equalTo(previewerId));
  }

  @Test
  public void testSendPipelineMetrics() throws Exception {
    PipelineState pipelineState = new PipelineStateImpl("user1",
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
    MockRemoteDataCollector remoteDataCollector = Mockito.spy(new MockRemoteDataCollector());
    Mockito.when(remoteDataCollector.getRemotePipelines()).thenReturn(Arrays.asList(pipelineState));
    MockPingFrequencyAdjustmentSenderReceiver eventClient = Mockito.spy(new MockPingFrequencyAdjustmentSenderReceiver());
    String jobRunnerUrl = "fakeUrl";
    HashMap<String, String> requestHeader = new HashMap<>();
    RemoteEventHandlerTask.sendPipelineMetrics(remoteDataCollector,
        eventClient,
        jobRunnerUrl,
        requestHeader,
        5);
    Mockito.verify(eventClient, Mockito.times(1)).submit(Mockito.anyString(), Mockito.anyMap(), Mockito.anyMap(), Mockito.anyList(), Mockito.anyInt());
  }

  @Test
  public void testSyncSenderMultiplePipelineStatus() throws Exception {
    EventClient eventClient = Mockito.mock(EventClient.class);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    RemoteDataCollector remoteDataCollector = Mockito.mock(RemoteDataCollector.class);
    Configuration conf = new Configuration();
    conf.set(RemoteEventHandlerTask.SHOULD_SEND_SYNC_EVENTS, true);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        remoteDataCollector,
        new SafeScheduledExecutorService(1, "testSyncSender"),
        new SafeScheduledExecutorService(1, "testSyncSender"),
        Mockito.mock(StageLibraryTask.class),
        buildInfo,
        Mockito.mock(RuntimeInfo.class),
        conf
    );
    RemoteEventHandlerTask.SyncEventSender syncEventSender = remoteEventHandlerTask.new SyncEventSender(
        eventClient,
        remoteDataCollector,
        jsonDto,
        60000,
        runtimeInfo,
        new SafeScheduledExecutorService(1, "testSyncSender"),
        Stopwatch.createUnstarted(),
        120000,
        70
    );
    syncEventSender.call();
    Mockito.verify(remoteDataCollector, Mockito.times(1)).getPipelines();
    Mockito.verify(eventClient, Mockito.times(1)).sendSyncEvents(
        Mockito.eq(remoteEventHandlerTask.getJobRunnerPipelineStatusEventsUrl()),
        Mockito.anyMap(),
        Mockito.anyMap(),
        Mockito.any(),
        Mockito.any(Long.class)
    );
    Mockito.verify(eventClient, Mockito.times(1)).sendSyncEvents(
        Mockito.eq(remoteEventHandlerTask.getSdcProcessMetricsEventUrl()),
        Mockito.anyMap(),
        Mockito.anyMap(),
        Mockito.any(),
        Mockito.any(Long.class)
    );
  }

  @Test
  public void testSyncSenderSinglePipelineStatus() throws Exception {
    EventClient eventClient = Mockito.mock(EventClient.class);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    RemoteDataCollector remoteDataCollector = Mockito.mock(RemoteDataCollector.class);
    Configuration conf = new Configuration();
    conf.set(RemoteEventHandlerTask.SHOULD_SEND_SYNC_EVENTS, true);
    final RemoteEventHandlerTask remoteEventHandlerTask = new RemoteEventHandlerTask(
        remoteDataCollector,
        new SafeScheduledExecutorService(1, "testSyncSender"),
        new SafeScheduledExecutorService(1, "testSyncSender"),
        Mockito.mock(StageLibraryTask.class),
        buildInfo,
        Mockito.mock(RuntimeInfo.class),
        conf
    );
    RemoteEventHandlerTask.SyncEventSender syncEventSender = remoteEventHandlerTask.new SyncEventSender(eventClient,
        remoteDataCollector,
        jsonDto,
        60000,
        runtimeInfo,
        new SafeScheduledExecutorService(1, "testSyncSender"),
        Stopwatch.createStarted(),
        120000,
        70
    );
    PipelineAndValidationStatus pipelineAndValidationStatus = new PipelineAndValidationStatus("",
        "",
        "",
        -1,
        false,
        null,
        null,
        null,
        false,
        null,
        null,
        0
    );
    Mockito.when(remoteDataCollector.getRemotePipelinesWithChanges()).thenReturn(Collections.singletonList(
        pipelineAndValidationStatus));
    syncEventSender.call();
    Mockito.verify(remoteDataCollector, Mockito.times(1)).getRemotePipelinesWithChanges();
    Mockito.verify(eventClient, Mockito.times(1))
        .sendSyncEvents(Mockito.eq(remoteEventHandlerTask.getJobRunnerPipelineStatusEventUrl()),
            Mockito.anyMap(),
            Mockito.anyMap(),
            Mockito.any(),
            Mockito.any(Long.class)
        );
  }

}
