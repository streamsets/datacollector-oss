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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.api.EventException;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvents;
import com.streamsets.datacollector.event.dto.SDCBuildInfo;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.ServerEvent;
import com.streamsets.datacollector.event.dto.StageInfo;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.AbstractSSOService;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RemoteEventHandlerTask extends AbstractTask implements EventHandlerTask {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteEventHandlerTask.class);
  private static final Long DEFAULT_PING_FREQUENCY = Long.valueOf(5000);
  private static final String REMOTE_CONTROL = AbstractSSOService.CONFIG_PREFIX + "remote.control.";
  public static final String REMOTE_JOB_LABELS = REMOTE_CONTROL + "job.labels";
  private static final String REMOTE_URL_PING_INTERVAL = REMOTE_CONTROL  + "ping.frequency";
  private static final String DEFAULT_REMOTE_JOB_LABELS = "";
  private static final String REMOTE_CONTROL_EVENTS_RECIPIENT = REMOTE_CONTROL + "events.recipient";
  private static final String DEFAULT_REMOTE_CONTROL_EVENTS_RECIPIENT = "jobrunner-app";
  private final DataCollector remoteDataCollector;
  private final EventClient eventSenderReceiver;
  private final MessagingJsonToFromDto jsonToFromDto;
  private final SafeScheduledExecutorService executorService;
  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;
  private final List<String> appDestinationList;
  private final List<String> labelList;
  private final Map<String, String> requestHeader;
  private final long defaultPingFrequency;

  public RemoteEventHandlerTask(DataCollector remoteDataCollector,
    EventClient eventSenderReceiver,
    SafeScheduledExecutorService executorService,
    StageLibraryTask stageLibrary,
    RuntimeInfo runtimeInfo,
    Configuration conf
    ) {
    super("REMOTE_EVENT_HANDLER");
    this.remoteDataCollector = remoteDataCollector;
    this.jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    this.executorService = executorService;
    this.eventSenderReceiver = eventSenderReceiver;
    this.stageLibrary = stageLibrary;
    this.runtimeInfo = runtimeInfo;
    appDestinationList = Arrays.asList(conf.get(
        REMOTE_CONTROL_EVENTS_RECIPIENT,
        DEFAULT_REMOTE_CONTROL_EVENTS_RECIPIENT
    ));
    String labels = conf.get(REMOTE_JOB_LABELS, DEFAULT_REMOTE_JOB_LABELS);
    labelList = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(labels));
    defaultPingFrequency = conf.get(REMOTE_URL_PING_INTERVAL, DEFAULT_PING_FREQUENCY);
    requestHeader = new HashMap<>();
    requestHeader.put("X-Requested-By", "SDC");
    requestHeader.put("X-SS-App-Auth-Token", runtimeInfo.getAppAuthToken());
    requestHeader.put("X-SS-App-Component-Id", this.runtimeInfo.getId());
  }

  @Override
  public void runTask() {
    executorService.submit(new EventHandlerCallable(remoteDataCollector, eventSenderReceiver, jsonToFromDto,
      new ArrayList<ClientEvent>(), getStartupReportEvent(), executorService, defaultPingFrequency,
        appDestinationList, requestHeader));

  }

  private ClientEvent getStartupReportEvent() {
    List<StageInfo> stageInfoList = new ArrayList<StageInfo>();
    for (StageDefinition stageDef : stageLibrary.getStages()) {
      stageInfoList.add(new StageInfo(stageDef.getName(), stageDef.getVersion(), stageDef.getLibrary()));
    }
    BuildInfo buildInfo = new DataCollectorBuildInfo();
    SDCInfoEvent sdcInfoEvent =
      new SDCInfoEvent(runtimeInfo.getId(), runtimeInfo.getBaseHttpUrl(), System.getProperty("java.runtime.version"),
        stageInfoList, new SDCBuildInfo(buildInfo.getVersion(), buildInfo.getBuiltBy(), buildInfo.getBuiltDate(),
          buildInfo.getBuiltRepoSha(), buildInfo.getSourceMd5Checksum()), labelList);
    return new ClientEvent(UUID.randomUUID().toString(), appDestinationList, false, false, EventType.SDC_INFO_EVENT, sdcInfoEvent, null);
  }

  @Override
  public void stopTask() {
    executorService.shutdownNow();
  }

  @VisibleForTesting
  static class EventHandlerCallable implements Callable<Void> {
    private final DataCollector remoteDataCollector;
    private final EventClient eventClient;
    private final MessagingJsonToFromDto jsonToFromDto;
    private final SafeScheduledExecutorService executorService;
    private final Map<String, String> requestHeader;
    private final List<String> jobEventDestinationList;
    private List<ClientEvent> ackEventList;
    private ClientEvent sdcInfoEvent;
    private long delay;

    public EventHandlerCallable(
        DataCollector remoteDataCollector,
        EventClient eventSenderReceiver,
        MessagingJsonToFromDto jsonToFromDto,
        List<ClientEvent> ackEventList,
        ClientEvent sdcInfoEvent,
        SafeScheduledExecutorService executorService,
        long delay,
        List<String> jobEventDestinationList,
        Map<String, String> requestHeader
    ) {
      this.remoteDataCollector = remoteDataCollector;
      this.eventClient = eventSenderReceiver;
      this.jsonToFromDto = jsonToFromDto;
      this.executorService = executorService;
      this.delay = delay;
      this.jobEventDestinationList = jobEventDestinationList;
      this.ackEventList = ackEventList;
      this.sdcInfoEvent = sdcInfoEvent;
      this.requestHeader = requestHeader;
    }

    @Override
    public Void call() {
      try {
        callRemoteControl();
      } catch (Exception ex) {
        LOG.error(Utils.format("Unexpected exception: '{}'", ex), ex);
      } finally {
        executorService.schedule(new EventHandlerCallable(remoteDataCollector, eventClient, jsonToFromDto, ackEventList,
          sdcInfoEvent, executorService, delay, jobEventDestinationList, requestHeader), delay, TimeUnit.MILLISECONDS);
      }
      return null;
    }

    @VisibleForTesting
    long getDelay() {
      return this.delay;
    }

    @VisibleForTesting
    List<ClientEvent> getAckEventList(){
      return ackEventList;
    }

    @VisibleForTesting
    void callRemoteControl() {
      List<ClientEvent> clientEventList = new ArrayList<>();
      for (ClientEvent ackEvent: ackEventList) {
        clientEventList.add(ackEvent);
      }
      if (sdcInfoEvent != null) {
        clientEventList.add(sdcInfoEvent);
      }
      try {
        List<PipelineStatusEvent> pipelineStatusEventList = new ArrayList<>();
        for (PipelineAndValidationStatus pipelineAndValidationStatus : remoteDataCollector.getPipelines()) {
          PipelineStatusEvent pipelineStatusEvent =
            new PipelineStatusEvent(pipelineAndValidationStatus.getName(), pipelineAndValidationStatus.getRev(),
              pipelineAndValidationStatus.isRemote(), pipelineAndValidationStatus.getPipelineStatus(),
              pipelineAndValidationStatus.getMessage(), pipelineAndValidationStatus.getWorkerInfos(),
              pipelineAndValidationStatus.getValidationStatus(),
              jsonToFromDto.serialize(BeanHelper.wrapIssues(pipelineAndValidationStatus.getIssues())),
              pipelineAndValidationStatus.isClusterMode());
          pipelineStatusEventList.add(pipelineStatusEvent);
        }
        PipelineStatusEvents pipelineStatusEvents = new PipelineStatusEvents();
        pipelineStatusEvents.setPipelineStatusEventList(pipelineStatusEventList);
        clientEventList.add(new ClientEvent(UUID.randomUUID().toString(), jobEventDestinationList, false, false,
            EventType.STATUS_MULTIPLE_PIPELINES, pipelineStatusEvents, null));
      } catch (Exception ex) {
        LOG.warn(Utils.format("Error while creating/serializing pipeline status event: '{}'", ex), ex);
      }
      List<ServerEventJson> serverEventJsonList;
      try {
        List<ClientEventJson> clientEventJsonList = jsonToFromDto.toJson(clientEventList);
        serverEventJsonList  = eventClient.submit("", new HashMap<String, String>(), requestHeader, false, clientEventJsonList);
      } catch (IOException | EventException e) {
        LOG.warn("Error while sending/receiving events to server:  " + e, e);
        return;
      }
      List<ClientEvent> ackClientEventList = new ArrayList<ClientEvent>();
      LOG.debug(Utils.format("Got '{}' events ", serverEventJsonList.size()));
      for (ServerEventJson serverEventJson : serverEventJsonList) {
        ClientEvent clientEvent = handlePipelineEvent(serverEventJson);
        if (clientEvent != null) {
          ackClientEventList.add(clientEvent);
        }
      }
      ackEventList = ackClientEventList;
      sdcInfoEvent = null;
    }

    private ClientEvent handlePipelineEvent(ServerEventJson serverEventJson) {
      String ackEventMessage = null;
      ServerEvent serverEvent = null;
      try {
        serverEvent = jsonToFromDto.asDto(serverEventJson);
        Event event = serverEvent.getEvent();
        EventType eventType = serverEvent.getEventType();
        LOG.info(Utils.format("Handling event of type: '{}' ", eventType));
        switch (eventType) {
          case PING_FREQUENCY_ADJUSTMENT:
            delay = ((PingFrequencyAdjustmentEvent)event).getPingFrequency();
            break;
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
          case STOP_DELETE_PIPELINE:
            PipelineBaseEvent pipelineStopDeleteEvent = (PipelineBaseEvent) event;
            remoteDataCollector.stopAndDelete(pipelineStopDeleteEvent.getUser(), pipelineStopDeleteEvent.getName(),
              pipelineStopDeleteEvent.getRev());
            break;
          default:
            ackEventMessage = Utils.format("Unrecognized event: '{}'", eventType);
            LOG.warn(ackEventMessage);
            break;
        }
      } catch (PipelineException | StageException ex) {
        ackEventMessage = Utils.format("Remote event type: '{}' encountered exception '{}'", EventType.fromValue(serverEventJson.getEventTypeId()), ex.getMessage());
        LOG.warn(ackEventMessage, ex);
      } catch (IOException ex) {
        ackEventMessage =
          Utils.format("Remote event type: '{}' encountered exception while being deserialized '{}'", EventType.fromValue(serverEventJson.getEventTypeId()), ex.getMessage());
        LOG.warn(ackEventMessage, ex);
      }
      if (serverEventJson.isRequiresAck()) {
        AckEvent ackEvent = new AckEvent(ackEventMessage == null ? AckEventStatus.SUCCESS : AckEventStatus.ERROR, ackEventMessage);
        return new ClientEvent(serverEventJson.getEventId(), jobEventDestinationList, false, true, EventType.ACK_EVENT,
          ackEvent, null);
      } else {
        return null;
      }
    }
  }
}
