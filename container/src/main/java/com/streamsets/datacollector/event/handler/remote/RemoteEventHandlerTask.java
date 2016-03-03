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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.event.EventException;
import com.streamsets.datacollector.event.EventClient;
import com.streamsets.datacollector.event.binding.JsonToFromDto;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.SDCBuildInfo;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.StageInfo;
import com.streamsets.datacollector.event.dto.UUIDEvent;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.json.AckEventJson;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.EventTypeJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.SDCInfoEventJson;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

public class RemoteEventHandlerTask extends AbstractTask implements EventHandlerTask {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteEventHandlerTask.class);
  private static final Long DEFAULT_PING_INTERVAL = Long.valueOf(10000);
  public static final String REMOTE_CONTROL_URL = "remote.control.url";
  private final DataCollector remoteDataCollector;
  private final EventClient eventSenderReceiver;
  private final JsonToFromDto jsonToFromDto;
  private final SafeScheduledExecutorService executorService;
  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;

  public RemoteEventHandlerTask(DataCollector remoteDataCollector,
    EventClient eventSenderReceiver,
    SafeScheduledExecutorService executorService,
    StageLibraryTask stageLibrary,
    RuntimeInfo runtimeInfo
    ) {
    super("REMOTE_EVENT_HANDLER");
    this.remoteDataCollector = remoteDataCollector;
    this.jsonToFromDto = JsonToFromDto.getInstance();
    this.executorService = executorService;
    this.eventSenderReceiver = eventSenderReceiver;
    this.stageLibrary = stageLibrary;
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void runTask() {
    executorService.submit(new EventHandlerCallable(remoteDataCollector, eventSenderReceiver, jsonToFromDto,
      new ArrayList<AckEventJson>(), getStartupReportEvent(), executorService, DEFAULT_PING_INTERVAL));

  }

  private SDCInfoEventJson getStartupReportEvent() {
    List<StageInfo> stageInfoList = new ArrayList<StageInfo>();
    for (StageDefinition stageDef : stageLibrary.getStages()) {
      stageInfoList.add(new StageInfo(stageDef.getName(), stageDef.getVersion(), stageDef.getLibrary()));
    }
    BuildInfo buildInfo = new DataCollectorBuildInfo();
    SDCInfoEvent sdcInfoEvent =
      new SDCInfoEvent(runtimeInfo.getId(), runtimeInfo.getBaseHttpUrl(), System.getProperty("java.runtime.version"),
        stageInfoList, new SDCBuildInfo(buildInfo.getVersion(), buildInfo.getBuiltBy(), buildInfo.getBuiltDate(),
          buildInfo.getBuiltRepoSha(), buildInfo.getSourceMd5Checksum()));
    return (SDCInfoEventJson) jsonToFromDto.toJson(EventType.SDC_INFO_EVENT, sdcInfoEvent);
  }

  @Override
  public void stopTask() {
    executorService.shutdownNow();
  }

  @VisibleForTesting
  static class EventHandlerCallable implements Callable<Void> {
    private final DataCollector remoteDataCollector;
    private final EventClient eventSenderReceiver;
    private final JsonToFromDto jsonToFromDto;
    private final SafeScheduledExecutorService executorService;
    private List<AckEventJson> ackEventList;
    private SDCInfoEventJson sdcInfoEvent;
    private long delay;

    public EventHandlerCallable(DataCollector remoteDataCollector,
      EventClient eventSenderReceiver,
      JsonToFromDto jsonToFromDto, List<AckEventJson> ackEventList,
      SDCInfoEventJson sdcInfoEvent,
      SafeScheduledExecutorService executorService,
      long delay) {
      this.remoteDataCollector = remoteDataCollector;
      this.eventSenderReceiver = eventSenderReceiver;
      this.jsonToFromDto = jsonToFromDto;
      this.executorService = executorService;
      this.delay = delay;
      this.ackEventList = ackEventList;
      this.sdcInfoEvent = sdcInfoEvent;
    }

    @Override
    public Void call() {
      try {
        callRemoteControl();
      } catch (Exception ex) {
        LOG.error(Utils.format("Unexpected exception: '{}'", ex), ex);
      } finally {
        executorService.schedule(new EventHandlerCallable(remoteDataCollector, eventSenderReceiver, jsonToFromDto, ackEventList,
          sdcInfoEvent, executorService, delay), delay, TimeUnit.MILLISECONDS);
      }
      return null;
    }

    @VisibleForTesting
    long getDelay() {
      return this.delay;
    }

    @VisibleForTesting
    List<AckEventJson> getAckEventList(){
      return ackEventList;
    }

    @VisibleForTesting
    void callRemoteControl() {
      Map<EventTypeJson, List<? extends EventJson>> sendingEventMap = new HashMap<EventTypeJson, List<? extends EventJson>>();
      if (!ackEventList.isEmpty()) {
        sendingEventMap.put(jsonToFromDto.toJson(EventType.ACK_EVENT), ackEventList);
      }
      if (sdcInfoEvent != null) {
        sendingEventMap.put(jsonToFromDto.toJson(EventType.SDC_INFO_EVENT), Arrays.asList(sdcInfoEvent));
      }
      try {
        List<PipelineStatusEventJson> statusJsonList = new ArrayList<PipelineStatusEventJson>();
        for (PipelineAndValidationStatus pipelineAndValidationStatus : remoteDataCollector.getPipelines()) {
          PipelineStatusEvent pipelineStatusEvent =
            new PipelineStatusEvent(pipelineAndValidationStatus.getName(), pipelineAndValidationStatus.getRev(),
              pipelineAndValidationStatus.isRemote(), pipelineAndValidationStatus.getPipelineStatus(),
              pipelineAndValidationStatus.getMessage(), pipelineAndValidationStatus.getValidationStatus(),
              jsonToFromDto.serialize(BeanHelper.wrapIssues(pipelineAndValidationStatus.getIssues())));
          statusJsonList.add((PipelineStatusEventJson)jsonToFromDto.toJson(EventType.STATUS_PIPELINE, pipelineStatusEvent));
        }
        if (!statusJsonList.isEmpty()) {
          sendingEventMap.put(jsonToFromDto.toJson(EventType.STATUS_PIPELINE), statusJsonList);
        }
      } catch (Exception ex) {
        LOG.warn(Utils.format("Error while creating/serializing pipeline status event: '{}'", ex), ex);
      }
      Map<EventTypeJson, List<? extends EventJson>> responseEventMap;
      try {
        responseEventMap = eventSenderReceiver.submit(sendingEventMap);
      } catch (EventException e) {
        LOG.warn("Error while sending/receiving events to server:  " + e, e);
        return;
      }
      List<AckEventJson> ackEventListJson = new ArrayList<AckEventJson>();
      for (Map.Entry<EventTypeJson, List<? extends EventJson>> mapEntry : responseEventMap.entrySet()) {
        EventType eventType = jsonToFromDto.asDto(mapEntry.getKey());
        for (EventJson eventJson : mapEntry.getValue()) {
          Event event = jsonToFromDto.asDto(eventType, eventJson);
          AckEvent ackEvent;
          if (eventType == EventType.PING_FREQUENCY_ADJUSTMENT) {
            delay = ((PingFrequencyAdjustmentEvent) event).getPingFrequency();
            ackEvent = new AckEvent(((UUIDEvent) event).getUuid(), AckEventStatus.SUCCESS, null);
          } else {
            ackEvent = handlePipelineEvent(eventType, event);
          }
          ackEventListJson.add((AckEventJson)jsonToFromDto.toJson(EventType.ACK_EVENT, ackEvent));
        }
      }
      ackEventList = ackEventListJson;
      sdcInfoEvent = null;
    }

    private AckEvent handlePipelineEvent(EventType eventType, Event event) {
      String ackEventMessage = null;
      try {
        switch (eventType) {
          case SAVE_PIPELINE: {
            PipelineSaveEvent pipelineSaveEvent = (PipelineSaveEvent) event;
            PipelineConfigAndRules pipelineConfigAndRules = pipelineSaveEvent.getPipelineConfigurationAndRules();
            TypeReference<PipelineConfigurationJson> typeRef =
              new TypeReference<PipelineConfigurationJson>() {};
            PipelineConfigurationJson pipelineConfigJson =
              jsonToFromDto.deserialize(pipelineConfigAndRules.getPipelineConfig(), typeRef);
            RuleDefinitionsJson ruleDefinitionsJson =
              jsonToFromDto.deserialize(pipelineConfigAndRules.getPipelineRules(), new TypeReference<RuleDefinitionsJson>() {
              });
            remoteDataCollector.savePipeline(pipelineSaveEvent.getUser(), pipelineSaveEvent.getName(),
              pipelineSaveEvent.getRev(), pipelineSaveEvent.getDescription(),
              BeanHelper.unwrapPipelineConfiguration(pipelineConfigJson), BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson));
            break;
          }
          case SAVE_RULES_PIPELINE: {
            PipelineSaveRulesEvent pipelineSaveRulesEvent = (PipelineSaveRulesEvent) event;
            RuleDefinitionsJson ruleDefinitionsJson =
              jsonToFromDto.deserialize(pipelineSaveRulesEvent.getRuleDefinitions(), new TypeReference<RuleDefinitionsJson>() {
              });
            remoteDataCollector.savePipelineRules(pipelineSaveRulesEvent.getName(), pipelineSaveRulesEvent.getRev(),
              BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson));
            break;
          }
          case START_PIPELINE:
            PipelineBaseEvent pipelineStartEvent = (PipelineBaseEvent) event;
            remoteDataCollector.start(pipelineStartEvent.getUser(), pipelineStartEvent.getName(), pipelineStartEvent.getRev());
            break;
          case STOP_PIPELINE:
            PipelineBaseEvent pipelineStopEvent = (PipelineBaseEvent) event;
            remoteDataCollector.stop(pipelineStopEvent.getUser(), pipelineStopEvent.getName(), pipelineStopEvent.getRev());
            break;
          case VALIDATE_PIPELINE:
            PipelineBaseEvent pipelineValidataEvent = (PipelineBaseEvent) event;
            remoteDataCollector.validateConfigs(pipelineValidataEvent.getUser(), pipelineValidataEvent.getName(),
              pipelineValidataEvent.getRev());
            break;
          case RESET_OFFSET_PIPELINE:
            PipelineBaseEvent pipelineResetOffsetEvent = (PipelineBaseEvent) event;
            remoteDataCollector.resetOffset(pipelineResetOffsetEvent.getUser(), pipelineResetOffsetEvent.getName(),
              pipelineResetOffsetEvent.getRev());
            break;
          case DELETE_HISTORY_PIPELINE:
            PipelineBaseEvent pipelineDeleteHistoryEvent = (PipelineBaseEvent) event;
            remoteDataCollector.deleteHistory(pipelineDeleteHistoryEvent.getUser(), pipelineDeleteHistoryEvent.getName(),
              pipelineDeleteHistoryEvent.getRev());
            break;
          case DELETE_PIPELINE:
            PipelineBaseEvent pipelineDeleteEvent = (PipelineBaseEvent) event;
            remoteDataCollector.delete(pipelineDeleteEvent.getName(), pipelineDeleteEvent.getRev());
            break;
          default:
            ackEventMessage = Utils.format("Unrecognized event: '{}'", eventType);
            LOG.warn(ackEventMessage);
            break;
        }
      } catch (PipelineException | StageException ex) {
        ackEventMessage = Utils.format("Remote event type: '{}' encountered exception '{}'", eventType, ex.getMessage());
        LOG.warn(ackEventMessage, ex);
      } catch (IOException ex) {
        ackEventMessage =
          Utils.format("Remote event type: '{}' encountered exception while being deserialized '{}'", eventType, ex.getMessage());
        LOG.warn(ackEventMessage, ex);
      }
      AckEvent ackEvent = new AckEvent(((UUIDEvent)event).getUuid(), ackEventMessage == null ? AckEventStatus.SUCCESS: AckEventStatus.ERROR, ackEventMessage);
      return ackEvent;
    }

  }
}
