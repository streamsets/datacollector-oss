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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

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
import com.streamsets.datacollector.event.EventClient;
import com.streamsets.datacollector.event.binding.JsonToFromDto;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.SDCBuildInfo;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.StageInfo;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.remote.PipelineAndValidationStatus;
import com.streamsets.datacollector.event.handler.remote.RemoteEventHandlerTask.EventHandlerCallable;
import com.streamsets.datacollector.event.json.AckEventJson;
import com.streamsets.datacollector.event.json.AckEventStatusJson;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.EventTypeJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.SDCInfoEventJson;
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
  private static final long PING_FREQUENCY = 10;

  private static class MockBaseEventSenderReceiver implements EventClient {
    public Map<EventTypeJson, List<? extends EventJson>> senderMap;

    @Override
    public Map<EventTypeJson, List<? extends EventJson>> submit(Map<EventTypeJson, List<? extends EventJson>> senderMap) {
      this.senderMap = senderMap;
      List<PipelineBaseEventJson> list = new ArrayList<PipelineBaseEventJson>();
      List<PipelineBaseEventJson> list2 = new ArrayList<PipelineBaseEventJson>();
      PipelineBaseEventJson pipelineBaseEventJson = new PipelineBaseEventJson();
      pipelineBaseEventJson.setName("name");
      pipelineBaseEventJson.setRev("rev");
      pipelineBaseEventJson.setUser("user");
      pipelineBaseEventJson.setUuid(id1);
      list.add(pipelineBaseEventJson);
      PipelineBaseEventJson pipelineBaseEventJson2 = new PipelineBaseEventJson();
      pipelineBaseEventJson2.setName("name1");
      pipelineBaseEventJson2.setRev("rev1");
      pipelineBaseEventJson2.setUser("user1");
      pipelineBaseEventJson2.setUuid(id2);
      list2.add(pipelineBaseEventJson);
      list2.add(pipelineBaseEventJson2);
      Map<EventTypeJson, List<? extends EventJson>> map = new HashMap<EventTypeJson, List<? extends EventJson>>();
      // add two events for start event
      map.put(EventTypeJson.START_PIPELINE, list2);
      map.put(EventTypeJson.STOP_PIPELINE, list);
      map.put(EventTypeJson.DELETE_PIPELINE, list);
      map.put(EventTypeJson.DELETE_HISTORY_PIPELINE, list);
      map.put(EventTypeJson.VALIDATE_PIPELINE, list);
      map.put(EventTypeJson.RESET_OFFSET_PIPELINE, list);
      return map;
    }

  }

  private static class MockPingFrequencyAdjustmentSenderReceiver implements EventClient {
    @Override
    public Map<EventTypeJson, List<? extends EventJson>> submit(Map<EventTypeJson, List<? extends EventJson>> senderMap) {
      List<PingFrequencyAdjustmentEventJson> list = new ArrayList<PingFrequencyAdjustmentEventJson>();
      PingFrequencyAdjustmentEventJson pingFrequencyJson = new PingFrequencyAdjustmentEventJson();
      pingFrequencyJson.setPingFrequency(PING_FREQUENCY);
      pingFrequencyJson.setUuid(id1);
      list.add(pingFrequencyJson);
      Map<EventTypeJson, List<? extends EventJson>> map = new HashMap<EventTypeJson, List<? extends EventJson>>();
      map.put(EventTypeJson.PING_FREQUENCY_ADJUSTMENT, list);
      return map;
    }

  }

  private static class MockSaveEventSenderReceiver implements EventClient {
    @Override
    public Map<EventTypeJson, List<? extends EventJson>> submit(Map<EventTypeJson, List<? extends EventJson>> senderMap) {
      try {
        Map<EventTypeJson, List<? extends EventJson>> map = new HashMap<EventTypeJson, List<? extends EventJson>>();
        List<PipelineSaveEventJson> list1 = new ArrayList<PipelineSaveEventJson>();
        List<PipelineSaveRulesEventJson> list2 = new ArrayList<PipelineSaveRulesEventJson>();
        PipelineSaveEventJson pipelineSaveEventJson = new PipelineSaveEventJson();
        pipelineSaveEventJson.setName("name");
        pipelineSaveEventJson.setRev("rev");
        pipelineSaveEventJson.setUser("user");
        pipelineSaveEventJson.setDescription("description");
        pipelineSaveEventJson.setUuid(id1);
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
        configRulesJson.setPipelineConfig(JsonToFromDto.getInstance().serialize(pipelineConfigJson));
        List<MetricsRuleDefinition> metricRulesList = new ArrayList<MetricsRuleDefinition>();
        ;
        metricRulesList.add(new MetricsRuleDefinition("id", "alertText", "metricId", MetricType.GAUGE,
          MetricElement.COUNTER_COUNT, "condition", false, true));
        RuleDefinitions ruleDefinitions =
          new RuleDefinitions(metricRulesList, new ArrayList<DataRuleDefinition>(), new ArrayList<DriftRuleDefinition>(),
            new ArrayList<String>(), id1);
        configRulesJson.setPipelineRules(JsonToFromDto.getInstance().serialize(BeanHelper.wrapRuleDefinitions(ruleDefinitions)));
        pipelineSaveEventJson.setPipelineConfigurationAndRules(configRulesJson);
        PipelineSaveRulesEventJson pipelineSaveRulesEventJson = new PipelineSaveRulesEventJson();
        pipelineSaveRulesEventJson.setName("name");
        pipelineSaveRulesEventJson.setRev("rev");
        pipelineSaveRulesEventJson.setUser("user");
        pipelineSaveRulesEventJson.setUuid(id1);
        pipelineSaveRulesEventJson.setRuleDefinitions(JsonToFromDto.getInstance().serialize(
          BeanHelper.wrapRuleDefinitions(ruleDefinitions)));
        list1.add(pipelineSaveEventJson);
        list2.add(pipelineSaveRulesEventJson);
        map.put(EventTypeJson.SAVE_PIPELINE, list1);
        map.put(EventTypeJson.SAVE_RULES_PIPELINE, list2);
        return map;
      } catch (Exception e) {
        throw new IllegalStateException("Cannot create event for test case");
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
  }

  @Test
  public void testPipelineBaseEventTriggered() {
    JsonToFromDto jsonToFromDto = JsonToFromDto.getInstance();
    List<AckEventJson> ackEventJsonList = new ArrayList<AckEventJson>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, new MockBaseEventSenderReceiver(), jsonToFromDto, ackEventJsonList, null,
        null, -1);
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<AckEventJson> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(7, ackEventList.size());
    int uuid1 = 0;
    int uuid2 = 0;
    for (AckEventJson ackEventJson : ackEventList) {
      if (id1 == ackEventJson.getUuid()) {
        uuid1++;
      } else if (id2 == ackEventJson.getUuid()) {
        uuid2++;
      }
      assertEquals(AckEventStatusJson.SUCCESS, ackEventJson.getAckEventStatus());
    }
    assertEquals(6, uuid1);
    assertEquals(1, uuid2);
    assertEquals(2, mockRemoteDataCollector.startCalled);
    assertTrue(mockRemoteDataCollector.stopCalled);
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
    JsonToFromDto jsonToFromDto = JsonToFromDto.getInstance();
    List<AckEventJson> ackEventJsonList = new ArrayList<AckEventJson>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, new MockSaveEventSenderReceiver(), jsonToFromDto, ackEventJsonList, null,
        null, -1);
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<AckEventJson> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(2, ackEventList.size());
    assertEquals(id1, ackEventList.get(0).getUuid());
    assertEquals(id1, ackEventList.get(1).getUuid());
    assertTrue(mockRemoteDataCollector.savePipelineCalled);
    assertTrue(mockRemoteDataCollector.savePipelineRulesCalled);
    assertEquals(AckEventStatusJson.SUCCESS, ackEventList.get(0).getAckEventStatus());
    assertEquals(AckEventStatusJson.SUCCESS, ackEventList.get(1).getAckEventStatus());
  }

  @Test
  public void testPipelineAckEventError() {
    JsonToFromDto jsonToFromDto = JsonToFromDto.getInstance();
    List<AckEventJson> ackEventJsonList = new ArrayList<AckEventJson>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, new MockBaseEventSenderReceiver(), jsonToFromDto, ackEventJsonList, null, null, -1);
    // start event in error
    mockRemoteDataCollector.errorInjection = true;
    remoteEventHandler.callRemoteControl();
    assertEquals(-1, remoteEventHandler.getDelay());
    List<AckEventJson> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(7, ackEventList.size());
    int error = 0;
    for (AckEventJson ackEventJson : ackEventList) {
      if (ackEventJson.getAckEventStatus() == AckEventStatusJson.ERROR) {
        error++;
      }
    }
    // 1 event in error
    assertEquals(2, error);
  }

  @Test
  public void testPingFrequencyEvent() {
    JsonToFromDto jsonToFromDto = JsonToFromDto.getInstance();
    List<AckEventJson> ackEventJsonList = new ArrayList<AckEventJson>();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(new MockRemoteDataCollector(), new MockPingFrequencyAdjustmentSenderReceiver(), jsonToFromDto,
        ackEventJsonList, null, null, -1);
    remoteEventHandler.callRemoteControl();
    assertEquals(PING_FREQUENCY, remoteEventHandler.getDelay());
    List<AckEventJson> ackEventList = remoteEventHandler.getAckEventList();
    assertEquals(1, ackEventList.size());
    assertEquals(id1, ackEventList.get(0).getUuid());
    assertEquals(AckEventStatusJson.SUCCESS, ackEventList.get(0).getAckEventStatus());
  }

  @Test
  public void testSendingEventClientToServer() {
    JsonToFromDto jsonToFromDto = JsonToFromDto.getInstance();
    List<AckEventJson> ackEventJsonList = new ArrayList<AckEventJson>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    mockRemoteDataCollector.putDummyPipelineStatus = true;
    MockBaseEventSenderReceiver mockBaseEventSenderReceiver = new MockBaseEventSenderReceiver();
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, mockBaseEventSenderReceiver, jsonToFromDto, ackEventJsonList, null, null, -1);
    remoteEventHandler.callRemoteControl();
    assertEquals(1, mockBaseEventSenderReceiver.senderMap.size());
    assertTrue(mockBaseEventSenderReceiver.senderMap.containsKey(EventTypeJson.STATUS_PIPELINE));
    List<? extends EventJson> pipelineStatusEventJsonList = mockBaseEventSenderReceiver.senderMap.get(EventTypeJson.STATUS_PIPELINE);
    assertEquals(2, pipelineStatusEventJsonList.size());
    PipelineStatusEventJson pipelineStatusEventJson = (PipelineStatusEventJson)pipelineStatusEventJsonList.get(0);
    assertEquals("name1", pipelineStatusEventJson.getName());
    assertEquals("rev1", pipelineStatusEventJson.getRev());
    assertEquals(PipelineStatusJson.RUNNING, pipelineStatusEventJson.getPipelineStatus());
    PipelineStatusEventJson pipelineStatusEventJson1 = (PipelineStatusEventJson)pipelineStatusEventJsonList.get(1);
    assertEquals("name2", pipelineStatusEventJson1.getName());
    assertEquals("rev2", pipelineStatusEventJson1.getRev());
    assertEquals(PipelineStatusJson.CONNECTING, pipelineStatusEventJson1.getPipelineStatus());
  }

  @Test
  public void testSendSDCInfoEvent() {
    JsonToFromDto jsonToFromDto = JsonToFromDto.getInstance();
    List<AckEventJson> ackEventJsonList = new ArrayList<AckEventJson>();
    MockRemoteDataCollector mockRemoteDataCollector = new MockRemoteDataCollector();
    MockBaseEventSenderReceiver mockBaseEventSenderReceiver = new MockBaseEventSenderReceiver();
    StageInfo stageInfo = new StageInfo("stage1", 1, "stageLib");
    List<StageInfo> stageInfoList = new ArrayList<StageInfo>();
    stageInfoList.add(stageInfo);
    SDCBuildInfo sdcBuildInfo = new SDCBuildInfo("1.0", "date1", "foo", "sha1", "checksum1");
    SDCInfoEvent sdcInfoEvent = new SDCInfoEvent("1", "localhost:9090", "1.7", stageInfoList, sdcBuildInfo);
    SDCInfoEventJson sdcInfoEventJson = (SDCInfoEventJson) jsonToFromDto.toJson(EventType.SDC_INFO_EVENT, sdcInfoEvent);
    EventHandlerCallable remoteEventHandler =
      new EventHandlerCallable(mockRemoteDataCollector, mockBaseEventSenderReceiver, jsonToFromDto, ackEventJsonList,
        sdcInfoEventJson, null, -1);
    remoteEventHandler.callRemoteControl();
    assertEquals(1, mockBaseEventSenderReceiver.senderMap.size());
    assertTrue(mockBaseEventSenderReceiver.senderMap.containsKey(EventTypeJson.SDC_INFO_EVENT));
    List<? extends EventJson> pipelineSDCInfoEventJsonList =
      mockBaseEventSenderReceiver.senderMap.get(EventTypeJson.SDC_INFO_EVENT);
    assertEquals(1, pipelineSDCInfoEventJsonList.size());
    SDCInfoEventJson sdcInfoJson = (SDCInfoEventJson) pipelineSDCInfoEventJsonList.get(0);
    assertEquals("1", sdcInfoJson.getSdcId());
    assertEquals("localhost:9090", sdcInfoJson.getHttpUrl());
    assertEquals("1.7", sdcInfoJson.getJavaVersion());
    List<StageInfoJson> stageInfoListJson = sdcInfoJson.getStageDefinitionList();
    assertEquals(1, stageInfoListJson.size());
    assertEquals("stage1", stageInfoListJson.get(0).getStageName());
    assertEquals(1, stageInfoListJson.get(0).getStageVersion());
    assertEquals("stageLib", stageInfoListJson.get(0).getLibraryName());
  }
}
