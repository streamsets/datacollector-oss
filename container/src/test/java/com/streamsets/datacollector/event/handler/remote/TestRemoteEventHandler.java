/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.api.EventException;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.SDCBuildInfo;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.StageInfo;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.remote.PipelineAndValidationStatus;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask.EventHandlerCallable;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.SDCInfoEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.datacollector.event.json.StageInfoJson;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.manager.PipelineManagerException;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;

public class TestRemoteEventHandler {

  private static final UUID id1 = UUID.randomUUID();
  private static final UUID id2 = UUID.randomUUID();
  private static final UUID id3 = UUID.randomUUID();
  private static final UUID id4 = UUID.randomUUID();
  private static final UUID id5 = UUID.randomUUID();
  private static final UUID id6 = UUID.randomUUID();
  private static final UUID id7 = UUID.randomUUID();

  private static final long PING_FREQUENCY = 10;
  private static final MessagingJsonToFromDto jsonDto = MessagingJsonToFromDto.INSTANCE;

  private static class MockBaseEventSenderReceiver implements EventClient {
    public List<ClientEventJson> clientJson;

    @Override
    public List<ServerEventJson> submit(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      boolean compression,
      List<ClientEventJson> clientEventJson) throws EventException {
      this.clientJson = clientEventJson;
      PipelineBaseEventJson pipelineBaseEventJson = new PipelineBaseEventJson();
      pipelineBaseEventJson.setName("name");
      pipelineBaseEventJson.setRev("rev");
      pipelineBaseEventJson.setUser("user");
      List<ServerEventJson> serverEventJsonList = new ArrayList<ServerEventJson>();
      try {
        ServerEventJson serverEventJson1 = new ServerEventJson();
        ServerEventJson serverEventJson2 = new ServerEventJson();
        ServerEventJson serverEventJson3 = new ServerEventJson();
        ServerEventJson serverEventJson4 = new ServerEventJson();
        ServerEventJson serverEventJson5 = new ServerEventJson();
        ServerEventJson serverEventJson6 = new ServerEventJson();
        ServerEventJson serverEventJson7 = new ServerEventJson();
        setServerEvent(serverEventJson1, id1.toString(), EventType.START_PIPELINE, false, true, jsonDto.serialize(pipelineBaseEventJson));
        setServerEvent(serverEventJson2, id2.toString(), EventType.STOP_PIPELINE, false, true, jsonDto.serialize(pipelineBaseEventJson));
        setServerEvent(serverEventJson3, id3.toString(), EventType.DELETE_PIPELINE, false, true, jsonDto.serialize(pipelineBaseEventJson));
        setServerEvent(serverEventJson4, id4.toString(), EventType.DELETE_HISTORY_PIPELINE, false, true,
          jsonDto.serialize(pipelineBaseEventJson));
        setServerEvent(serverEventJson5, id5.toString(), EventType.VALIDATE_PIPELINE, false, true, jsonDto.serialize(pipelineBaseEventJson));
        setServerEvent(serverEventJson6, id6.toString(), EventType.RESET_OFFSET_PIPELINE, false, true,
          jsonDto.serialize(pipelineBaseEventJson));
        setServerEvent(serverEventJson7, id7.toString(), EventType.STOP_DELETE_PIPELINE, false, true, jsonDto.serialize(pipelineBaseEventJson));

        serverEventJsonList.addAll(Arrays.asList(serverEventJson1, serverEventJson2, serverEventJson3, serverEventJson4, serverEventJson5,
          serverEventJson6, serverEventJson7));

      } catch (JsonProcessingException e) {
        throw new EventException("Cannot create event for test case" + e.getMessage());
      }
      return serverEventJsonList;
    }

  }

  private static void setServerEvent(ServerEventJson serverEventJson, String eventId, EventType eventType, boolean isAckEvent, boolean requiresAck,
    String payload) {
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
      List<ClientEventJson> clientEventJson) throws EventException {
      PingFrequencyAdjustmentEventJson pingFrequencyJson = new PingFrequencyAdjustmentEventJson();
      pingFrequencyJson.setPingFrequency(PING_FREQUENCY);
      ServerEventJson serverEventJson1 = new ServerEventJson();
      try {
        setServerEvent(serverEventJson1, id1.toString(), EventType.PING_FREQUENCY_ADJUSTMENT, false, true, jsonDto.serialize(pingFrequencyJson));
      } catch (Exception e) {
        throw new EventException(e.getMessage());
      }
      return Arrays.asList(serverEventJson1);
    }

  }

  private static class MockSaveEventSenderReceiver implements EventClient {
    @Override
    public List<ServerEventJson> submit(
      String path,
      Map<String, String> queryParams,
      Map<String, String> headerParams,
      boolean compression,
      List<ClientEventJson> clientEventJson) throws EventException {
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
        PipelineConfiguration pipelineConf =
          new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, PipelineConfigBean.VERSION, UUID.randomUUID(),
            "description", list, uiInfo, MockStages.getSourceStageConfig(), MockStages.getErrorStageConfig(), null);
        PipelineConfigurationJson pipelineConfigJson = BeanHelper.wrapPipelineConfiguration(pipelineConf);
        PipelineConfigAndRulesJson configRulesJson = new PipelineConfigAndRulesJson();
        configRulesJson.setPipelineConfig(MessagingJsonToFromDto.INSTANCE.serialize(pipelineConfigJson));
        List<MetricsRuleDefinition> metricRulesList = new ArrayList<MetricsRuleDefinition>();
        ;
        metricRulesList.add(new MetricsRuleDefinition("id", "alertText", "metricId", MetricType.GAUGE,
          MetricElement.COUNTER_COUNT, "condition", false, true, System.currentTimeMillis()));
        RuleDefinitions ruleDefinitions =
          new RuleDefinitions(metricRulesList, new ArrayList<DataRuleDefinition>(), new ArrayList<DriftRuleDefinition>(),
            new ArrayList<String>(), id1);
        configRulesJson.setPipelineRules(MessagingJsonToFromDto.INSTANCE.serialize(BeanHelper.wrapRuleDefinitions(ruleDefinitions)));
        pipelineSaveEventJson.setPipelineConfigurationAndRules(configRulesJson);
        PipelineSaveRulesEventJson pipelineSaveRulesEventJson = new PipelineSaveRulesEventJson();
        pipelineSaveRulesEventJson.setName("name");
        pipelineSaveRulesEventJson.setRev("rev");
        pipelineSaveRulesEventJson.setUser("user");
        pipelineSaveRulesEventJson.setRuleDefinitions(MessagingJsonToFromDto.INSTANCE.serialize(
          BeanHelper.wrapRuleDefinitions(ruleDefinitions)));
        ServerEventJson serverEventJson1 = new ServerEventJson();
        ServerEventJson serverEventJson2 = new ServerEventJson();
        setServerEvent(serverEventJson1, id1.toString(), EventType.SAVE_PIPELINE, false, true, jsonDto.serialize(pipelineSaveEventJson));
        setServerEvent(serverEventJson2, id2.toString(), EventType.SAVE_RULES_PIPELINE, false, true, jsonDto.serialize(pipelineSaveRulesEventJson));
        serverEventJsonList.addAll(Arrays.asList(serverEventJson1, serverEventJson2));
        return serverEventJsonList;

      } catch (Exception e) {
        throw new EventException("Cannot create event for test case " + e.getMessage());
      }
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

    @Override
    public void start(String user, String name, String rev) throws PipelineException, StageException {
      startCalled++;
      if (errorInjection) {
        throw new PipelineException(ContainerError.CONTAINER_0001);
      }
    }

    @Override
    public void stop(String user, String name, String rev) throws PipelineStoreException, PipelineManagerException,
      PipelineException {
      stopCalled = true;
    }

    @Override
    public void delete(String name, String rev) throws PipelineStoreException {
      deleteCalled = true;
    }

    @Override
    public void deleteHistory(String user, String name, String rev) throws PipelineStoreException, PipelineManagerException {
      deleteHistoryCalled = true;
    }

    @Override
    public void savePipeline(String user,
      String name,
      String rev,
      String description,
      PipelineConfiguration pipelineConfiguration,
      RuleDefinitions ruleDefinitions) throws PipelineStoreException {
      savePipelineCalled = true;
    }

    @Override
    public void savePipelineRules(String name, String rev, RuleDefinitions ruleDefinitions) throws PipelineStoreException {
      savePipelineRulesCalled = true;
    }

    @Override
    public void resetOffset(String user, String name, String rev) throws PipelineStoreException, PipelineRunnerException,
      PipelineManagerException {
      resetOffsetCalled = true;
    }

    @Override
    public void validateConfigs(String user, String name, String rev) throws PipelineException {
      validateConfigsCalled = true;
    }

    @Override
    public Collection<PipelineAndValidationStatus> getPipelines() throws PipelineStoreException {
      getPipelinesCalled = true;
      List<PipelineAndValidationStatus> list = new ArrayList<PipelineAndValidationStatus>();
      if (putDummyPipelineStatus) {
        list.add(new PipelineAndValidationStatus("name1", "rev1", false, PipelineStatus.RUNNING, "message"));
        list.add(new PipelineAndValidationStatus("name2", "rev2", false, PipelineStatus.CONNECTING, "message"));
      }
      return list;
    }

    @Override
    public void stopAndDelete(String user, String name, String rev) throws PipelineException, StageException {
      stopDeletePipelineCalled = true;
    }
  }

  @Test
  public void testPipelineBaseEventTriggered() {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, new MockBaseEventSenderReceiver(), jsonToFromDto, ackEventJsonList, null,
        null, -1, "JOB_RUNNER", new HashMap<String, String>());
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(7, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    assertTrue(ackEventList.get(0).getEvent() instanceof AckEvent);
    AckEvent ackEvent = (AckEvent)ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id2.toString(), ackEventList.get(1).getEventId());
    assertTrue(ackEventList.get(1).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent)ackEventList.get(1).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id3.toString(), ackEventList.get(2).getEventId());
    assertTrue(ackEventList.get(2).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent)ackEventList.get(2).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id4.toString(), ackEventList.get(3).getEventId());
    assertTrue(ackEventList.get(3).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent)ackEventList.get(3).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id5.toString(), ackEventList.get(4).getEventId());
    assertTrue(ackEventList.get(4).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent)ackEventList.get(4).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id6.toString(), ackEventList.get(5).getEventId());
    assertTrue(ackEventList.get(5).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent)ackEventList.get(5).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(id7.toString(), ackEventList.get(6).getEventId());
    assertTrue(ackEventList.get(6).getEvent() instanceof AckEvent);
    ackEvent = (AckEvent)ackEventList.get(6).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());

    assertEquals(1, mockRemoteDataCollector.startCalled);
    assertTrue(mockRemoteDataCollector.stopCalled);
    assertTrue(mockRemoteDataCollector.stopDeletePipelineCalled);
    assertTrue(mockRemoteDataCollector.resetOffsetCalled);
    assertTrue(mockRemoteDataCollector.validateConfigsCalled);
    assertTrue(mockRemoteDataCollector.deleteCalled);
    assertTrue(mockRemoteDataCollector.deleteHistoryCalled);
    assertTrue(mockRemoteDataCollector.getPipelinesCalled);
    assertFalse(mockRemoteDataCollector.savePipelineCalled);
    assertFalse(mockRemoteDataCollector.savePipelineRulesCalled);
  }

  @Test
  public void testPipelineSaveEventTriggered() {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, new MockSaveEventSenderReceiver(), jsonToFromDto, ackEventJsonList, null,
        null, -1, "JOB_RUNNER", new HashMap<String, String>());
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(2, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    AckEvent ackEvent = (AckEvent)ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());
    assertTrue(mockRemoteDataCollector.savePipelineCalled);
    assertTrue(mockRemoteDataCollector.savePipelineRulesCalled);
  }

  @Test
  public void testPipelineAckEventError() {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, new MockBaseEventSenderReceiver(), jsonToFromDto, ackEventJsonList, null,
        null, -1, "JOB_RUNNER", new HashMap<String, String>());
    // start event in error
    mockRemoteDataCollector.errorInjection = true;
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(7, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    assertTrue(ackEventList.get(0).getEvent() instanceof AckEvent);
    AckEvent ackEvent = (AckEvent)ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.ERROR, ackEvent.getAckEventStatus());
  }

  @Test
  public void testPingFrequencyEvent() {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(new MockRemoteDataCollector(), new MockPingFrequencyAdjustmentSenderReceiver(), jsonToFromDto,
        ackEventJsonList, null, null, -1, "JOB_RUNNER", new HashMap<String, String>());
    remoteEventHandler.callRemoteControl();
    assertEquals(PING_FREQUENCY, remoteEventHandler.getDelay());
    List<ClientEvent> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(1, ackEventList.size());
    assertEquals(id1.toString(), ackEventList.get(0).getEventId());
    assertTrue(ackEventList.get(0).getEvent() instanceof AckEvent);
    AckEvent ackEvent = (AckEvent)ackEventList.get(0).getEvent();
    assertEquals(AckEventStatus.SUCCESS, ackEvent.getAckEventStatus());
  }


  @Test
  public void testSendingEventClientToServer() throws Exception {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    mockRemoteDataCollector.putDummyPipelineStatus = true;
    MockBaseEventSenderReceiver mockBaseEventSenderReceiver = new MockBaseEventSenderReceiver();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, mockBaseEventSenderReceiver, jsonToFromDto, ackEventJsonList, null, null, -1,
        "JOB_RUNNER", new HashMap<String, String>());
    remoteEventHandler.callRemoteControl();
    assertEquals(2, mockBaseEventSenderReceiver.clientJson.size());
    ClientEventJson clientEventJson = mockBaseEventSenderReceiver.clientJson.get(0);
    PipelineStatusEventJson pipelineStatusEventJson =
      jsonToFromDto.deserialize(clientEventJson.getPayload(), new TypeReference<PipelineStatusEventJson>() {
      });
    assertEquals("name1", pipelineStatusEventJson.getName());
    assertEquals("rev1", pipelineStatusEventJson.getRev());
    assertEquals(PipelineStatusJson.RUNNING, pipelineStatusEventJson.getPipelineStatus());

    PipelineStatusEventJson pipelineStatusEventJson1 =
      jsonToFromDto.deserialize(mockBaseEventSenderReceiver.clientJson.get(1).getPayload(), new TypeReference<PipelineStatusEventJson>() {
      });
    assertEquals("name2", pipelineStatusEventJson1.getName());
    assertEquals("rev2", pipelineStatusEventJson1.getRev());
    assertEquals(PipelineStatusJson.CONNECTING, pipelineStatusEventJson1.getPipelineStatus());
  }

  @Test
  public void testSendSDCInfoEvent() throws Exception {
    MessagingJsonToFromDto jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    List<ClientEvent> ackEventJsonList = new ArrayList<ClientEvent>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    MockBaseEventSenderReceiver mockBaseEventSenderReceiver = new MockBaseEventSenderReceiver();
    StageInfo stageInfo = new StageInfo("stage1", 1, "stageLib");
    List<StageInfo> stageInfoList = new ArrayList<StageInfo>();
    stageInfoList.add(stageInfo);
    SDCBuildInfo sdcBuildInfo = new SDCBuildInfo("1.0", "date1", "foo", "sha1", "checksum1");
    SDCInfoEvent sdcInfoEvent =
      new SDCInfoEvent("1", "localhost:9090", "1.7", stageInfoList, sdcBuildInfo, Arrays.asList("label_1", "label_2"));
    ClientEvent clientEvent =
      new ClientEvent(id1.toString(), Arrays.asList("JOB_RUNNER"), false, false, EventType.SDC_INFO_EVENT, sdcInfoEvent, null);
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, mockBaseEventSenderReceiver, jsonToFromDto, ackEventJsonList, clientEvent, null,
        -1, "JOB_RUNNER", new HashMap<String, String>());
    remoteEventHandler.callRemoteControl();
    assertEquals(1, mockBaseEventSenderReceiver.clientJson.size());
    assertEquals(EventType.SDC_INFO_EVENT.getValue(), mockBaseEventSenderReceiver.clientJson.get(0).getEventTypeId());
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
  }
}
