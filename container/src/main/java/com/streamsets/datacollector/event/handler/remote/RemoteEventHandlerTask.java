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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.api.EventException;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.DisconnectedSsoCredentialsEvent;
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
import com.streamsets.datacollector.event.dto.SyncAclEvent;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.DataCollectorBuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.runner.production.SourceOffsetUpgrader;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.DisconnectedSecurityUtils;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.AbstractSSOService;
import com.streamsets.lib.security.http.DisconnectedSSOManager;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RemoteEventHandlerTask extends AbstractTask implements EventHandlerTask {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteEventHandlerTask.class);
  private static final long DEFAULT_PING_FREQUENCY = 5000;
  private static final long SYSTEM_LIMIT_MIN_PING_FREQUENCY = 5000;
  private static final long DEFAULT_STATUS_EVENTS_INTERVAL = 60000;
  private static final long SYSTEM_LIMIT_MIN_STATUS_EVENTS_INTERVAL = 30000;
  private static final String REMOTE_CONTROL = AbstractSSOService.CONFIG_PREFIX + "remote.control.";
  public static final String REMOTE_JOB_LABELS = REMOTE_CONTROL + "job.labels";
  private static final String REMOTE_URL_PING_INTERVAL = REMOTE_CONTROL  + "ping.frequency";
  private static final String REMOTE_URL_SEND_ALL_STATUS_EVENTS_INTERVAL = REMOTE_CONTROL + "status.events.interval";
  private static final String DEFAULT_REMOTE_JOB_LABELS = "all";
  private static final String REMOTE_CONTROL_EVENTS_RECIPIENT = REMOTE_CONTROL + "events.recipient";
  private static final String DEFAULT_REMOTE_CONTROL_EVENTS_RECIPIENT = "jobrunner-app";
  public static final String OFFSET = "offset";
  public static final int OFFSET_PROTOCOL_VERSION = 2;

  private final RemoteDataCollector remoteDataCollector;
  private final EventClient eventSenderReceiver;
  private final MessagingJsonToFromDto jsonToFromDto;
  private final SafeScheduledExecutorService executorService;
  private final StageLibraryTask stageLibrary;
  private final RuntimeInfo runtimeInfo;
  private final List<String> appDestinationList;
  private final List<String> labelList;
  private final Map<String, String> requestHeader;
  private final long defaultPingFrequency;
  private final Stopwatch stopWatch;
  private final long sendAllStatusEventsInterval;
  private final DataStore dataStore;


  public RemoteEventHandlerTask(RemoteDataCollector remoteDataCollector,
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
    defaultPingFrequency = Math.max(conf.get(REMOTE_URL_PING_INTERVAL, DEFAULT_PING_FREQUENCY),
        SYSTEM_LIMIT_MIN_PING_FREQUENCY
    );
    sendAllStatusEventsInterval = Math.max(
        conf.get(REMOTE_URL_SEND_ALL_STATUS_EVENTS_INTERVAL, DEFAULT_STATUS_EVENTS_INTERVAL),
        SYSTEM_LIMIT_MIN_STATUS_EVENTS_INTERVAL
    );
    requestHeader = new HashMap<>();
    requestHeader.put("X-Requested-By", "SDC");
    requestHeader.put("X-SS-App-Auth-Token", runtimeInfo.getAppAuthToken());
    requestHeader.put("X-SS-App-Component-Id", this.runtimeInfo.getId());
    stopWatch = Stopwatch.createUnstarted();

    File storeFile = new File(runtimeInfo.getDataDir(), DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_FILE);
    dataStore = new DataStore(storeFile);
    try {
      dataStore.exists(); // to trigger recovery if last write was incomplete
    } catch (IOException ex) {
      LOG.warn("Could not recover disconnected credentials file '{}': {}", dataStore.getFile(), ex.toString(), ex);
      try {
        dataStore.delete();
      } catch (IOException ex1) {
        throw new RuntimeException(Utils.format("Could not clear invalid disconected credentials file '{}': {}",
            dataStore.getFile(),
            ex.toString()),
            ex);
      }
    }
    remoteDataCollector.init();
  }

  @VisibleForTesting
  DataStore getDisconnectedSsoCredentialsDataStore() {
    return dataStore;
  }

  @Override
  public void runTask() {
    executorService.submit(new EventHandlerCallable(
        remoteDataCollector,
        eventSenderReceiver,
        jsonToFromDto,
        new ArrayList<>(),
        new ArrayList<>(),
        getStartupReportEvent(),
        executorService,
        defaultPingFrequency,
        appDestinationList,
        requestHeader,
        stopWatch,
        sendAllStatusEventsInterval,
        dataStore
    ));
  }

  private ClientEvent getStartupReportEvent() {
    List<StageInfo> stageInfoList = new ArrayList<StageInfo>();
    for (StageDefinition stageDef : stageLibrary.getStages()) {
      stageInfoList.add(new StageInfo(stageDef.getName(), stageDef.getVersion(), stageDef.getLibrary()));
    }
    BuildInfo buildInfo = new DataCollectorBuildInfo();
    SDCInfoEvent sdcInfoEvent = new SDCInfoEvent(runtimeInfo.getId(),
        runtimeInfo.getBaseHttpUrl(),
        System.getProperty("java.runtime.version"),
        stageInfoList,
        new SDCBuildInfo(buildInfo.getVersion(),
            buildInfo.getBuiltBy(),
            buildInfo.getBuiltDate(),
            buildInfo.getBuiltRepoSha(),
            buildInfo.getSourceMd5Checksum()
        ),
        labelList,
        OFFSET_PROTOCOL_VERSION,
        Strings.emptyToNull(runtimeInfo.getDeploymentId())
    );
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
    private final Stopwatch stopWatch;
    private final long waitBetweenSendingStatusEvents;
    private final DataStore disconnectedCredentialsDataStore;
    private List<ClientEvent> ackEventList;
    private List<ClientEvent> remoteEventList;
    private ClientEvent sdcInfoEvent;
    private long delay;

    public EventHandlerCallable(
        DataCollector remoteDataCollector,
        EventClient eventSenderReceiver,
        MessagingJsonToFromDto jsonToFromDto,
        List<ClientEvent> ackEventList,
        List<ClientEvent> remoteEventList,
        ClientEvent sdcInfoEvent,
        SafeScheduledExecutorService executorService,
        long delay,
        List<String> jobEventDestinationList,
        Map<String, String> requestHeader,
        Stopwatch stopWatch,
        long waitBetweenSendingStatusEvents,
        DataStore disconnectedCredentialsDataStore
        ) {
      this.remoteDataCollector = remoteDataCollector;
      this.eventClient = eventSenderReceiver;
      this.jsonToFromDto = jsonToFromDto;
      this.executorService = executorService;
      this.delay = delay;
      this.jobEventDestinationList = jobEventDestinationList;
      this.ackEventList = ackEventList;
      this.remoteEventList = remoteEventList;
      this.sdcInfoEvent = sdcInfoEvent;
      this.requestHeader = requestHeader;
      this.stopWatch = stopWatch;
      this.waitBetweenSendingStatusEvents = waitBetweenSendingStatusEvents;
      this.disconnectedCredentialsDataStore = disconnectedCredentialsDataStore;
    }

    @Override
    public Void call() {
      try {
        callRemoteControl();
      } catch (Exception ex) {
        LOG.warn("Cannot connect to send/receive events: {}", ex.toString());
      } finally {
        executorService.schedule(new EventHandlerCallable(remoteDataCollector,
            eventClient,
            jsonToFromDto,
            ackEventList,
            remoteEventList,
            sdcInfoEvent,
            executorService,
            delay,
            jobEventDestinationList,
            requestHeader,
            stopWatch,
            waitBetweenSendingStatusEvents,
            disconnectedCredentialsDataStore
        ), delay, TimeUnit.MILLISECONDS);
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

    private PipelineStatusEvent createPipelineStatusEvent(
        MessagingJsonToFromDto jsonToFromDto, PipelineAndValidationStatus pipelineAndValidationStatus
    ) throws JsonProcessingException {
      PipelineStatusEvent pipelineStatusEvent = new PipelineStatusEvent(
          pipelineAndValidationStatus.getName(),
          pipelineAndValidationStatus.getTitle(),
          pipelineAndValidationStatus.getRev(),
          pipelineAndValidationStatus.getTimeStamp(),
          pipelineAndValidationStatus.isRemote(),
          pipelineAndValidationStatus.getPipelineStatus(),
          pipelineAndValidationStatus.getMessage(),
          pipelineAndValidationStatus.getWorkerInfos(),
          pipelineAndValidationStatus.getValidationStatus(),
          jsonToFromDto.serialize(BeanHelper.wrapIssues(pipelineAndValidationStatus.getIssues())),
          pipelineAndValidationStatus.isClusterMode(),
          pipelineAndValidationStatus.getOffset(),
          OFFSET_PROTOCOL_VERSION,
          pipelineAndValidationStatus.getAcl(),
          pipelineAndValidationStatus.getRunnerCount()
      );
      return pipelineStatusEvent;
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
        if (!stopWatch.isRunning() || stopWatch.elapsed(TimeUnit.MILLISECONDS) > waitBetweenSendingStatusEvents) {
          // get state of all running pipeline and state of all remote pipelines
          List<PipelineStatusEvent> pipelineStatusEventList = new ArrayList<>();
          LOG.debug("Sending status of all pipelines");
          for (PipelineAndValidationStatus pipelineAndValidationStatus : remoteDataCollector.getPipelines()) {
            pipelineStatusEventList.add(createPipelineStatusEvent(jsonToFromDto, pipelineAndValidationStatus));
          }
          stopWatch.reset();
          PipelineStatusEvents pipelineStatusEvents = new PipelineStatusEvents();
          pipelineStatusEvents.setPipelineStatusEventList(pipelineStatusEventList);
          clientEventList.add(new ClientEvent(UUID.randomUUID().toString(),
              jobEventDestinationList,
              false,
              false,
              EventType.STATUS_MULTIPLE_PIPELINES,
              pipelineStatusEvents,
              null
          ));
          // Clear this state change list as we are fetching all events
          remoteEventList.clear();
        } else {
          // get state of only remote pipelines which changed state
          List<PipelineAndValidationStatus> pipelineAndValidationStatuses = remoteDataCollector
              .getRemotePipelinesWithChanges();
          for (PipelineAndValidationStatus pipelineAndValidationStatus : pipelineAndValidationStatuses) {
            PipelineStatusEvent pipelineStatusEvent = createPipelineStatusEvent(jsonToFromDto,
                pipelineAndValidationStatus);
            ClientEvent clientEvent = new ClientEvent(UUID.randomUUID().toString(),
                jobEventDestinationList,
                false,
                false,
                EventType.STATUS_PIPELINE,
                createPipelineStatusEvent(jsonToFromDto, pipelineAndValidationStatus),
                null
            );
            remoteEventList.add(clientEvent);
            LOG.debug(Utils.format("Sending event for remote pipeline: '{}' in status: '{}'",
                pipelineStatusEvent.getName(), pipelineStatusEvent.getPipelineStatus()));
          }
        }
      } catch (Exception ex) {
        LOG.warn(Utils.format("Error while creating/serializing pipeline status event: '{}'", ex), ex);
      }
      clientEventList.addAll(remoteEventList);
      List<ServerEventJson> serverEventJsonList;
      try {
        List<ClientEventJson> clientEventJsonList = jsonToFromDto.toJson(clientEventList);
        serverEventJsonList  = eventClient.submit("", new HashMap<String, String>(), requestHeader, false, clientEventJsonList);
        remoteEventList.clear();
        if (!stopWatch.isRunning()) {
          stopWatch.start();
        }
      } catch (IOException | EventException e) {
        LOG.warn("Error while sending/receiving events to server:  " + e, e);
        return;
      }
      List<ClientEvent> ackClientEventList = new ArrayList<ClientEvent>();
      for (ServerEventJson serverEventJson : serverEventJsonList) {
        ClientEvent clientEvent = handlePipelineEvent(serverEventJson);
        if (clientEvent != null) {
          ackClientEventList.add(clientEvent);
        }
      }
      ackEventList = ackClientEventList;
      sdcInfoEvent = null;
    }

    private String handleServerEvent(ServerEvent serverEvent) {
      String ackEventMessage = null;
      try {
        Event event = serverEvent.getEvent();
        EventType eventType = serverEvent.getEventType();
        LOG.info(Utils.format("Handling event of type: '{}' ", eventType));
        switch (eventType) {
          case PING_FREQUENCY_ADJUSTMENT:
            delay = ((PingFrequencyAdjustmentEvent) event).getPingFrequency();
            break;
          case SAVE_PIPELINE: {
            PipelineSaveEvent pipelineSaveEvent = (PipelineSaveEvent) event;
            PipelineConfigAndRules pipelineConfigAndRules = pipelineSaveEvent.getPipelineConfigurationAndRules();
            TypeReference<PipelineConfigurationJson> typeRef = new TypeReference<PipelineConfigurationJson>() {
            };
            PipelineConfigurationJson pipelineConfigJson = jsonToFromDto.deserialize(
                pipelineConfigAndRules.getPipelineConfig(),
                typeRef
            );
            RuleDefinitionsJson ruleDefinitionsJson = jsonToFromDto.deserialize(
                pipelineConfigAndRules.getPipelineRules(),
                new TypeReference<RuleDefinitionsJson>() {
                }
            );

            SourceOffset sourceOffset = getSourceOffset(pipelineSaveEvent);

            remoteDataCollector.savePipeline(pipelineSaveEvent.getUser(),
                pipelineSaveEvent.getName(),
                pipelineSaveEvent.getRev(),
                pipelineSaveEvent.getDescription(),
                sourceOffset,
                BeanHelper.unwrapPipelineConfiguration(pipelineConfigJson),
                BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson),
                pipelineSaveEvent.getAcl()
            );
            break;
          }
          case SAVE_RULES_PIPELINE: {
            PipelineSaveRulesEvent pipelineSaveRulesEvent = (PipelineSaveRulesEvent) event;
            RuleDefinitionsJson ruleDefinitionsJson = jsonToFromDto.deserialize(
                pipelineSaveRulesEvent.getRuleDefinitions(),
                new TypeReference<RuleDefinitionsJson>() {
                }
            );
            remoteDataCollector.savePipelineRules(pipelineSaveRulesEvent.getName(),
                pipelineSaveRulesEvent.getRev(),
                BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson)
            );
            break;
          }
          case START_PIPELINE:
            PipelineBaseEvent pipelineStartEvent = (PipelineBaseEvent) event;
            remoteDataCollector.start(
                pipelineStartEvent.getUser(),
                pipelineStartEvent.getName(),
                pipelineStartEvent.getRev()
            );
            break;
          case STOP_PIPELINE:
            PipelineBaseEvent pipelineStopEvent = (PipelineBaseEvent) event;
            remoteDataCollector.stop(
                pipelineStopEvent.getUser(),
                pipelineStopEvent.getName(),
                pipelineStopEvent.getRev()
            );
            break;
          case VALIDATE_PIPELINE:
            PipelineBaseEvent pipelineValidataEvent = (PipelineBaseEvent) event;
            remoteDataCollector.validateConfigs(pipelineValidataEvent.getUser(),
                pipelineValidataEvent.getName(),
                pipelineValidataEvent.getRev()
            );
            break;
          case RESET_OFFSET_PIPELINE:
            PipelineBaseEvent pipelineResetOffsetEvent = (PipelineBaseEvent) event;
            remoteDataCollector.resetOffset(pipelineResetOffsetEvent.getUser(),
                pipelineResetOffsetEvent.getName(),
                pipelineResetOffsetEvent.getRev()
            );
            break;
          case DELETE_HISTORY_PIPELINE:
            PipelineBaseEvent pipelineDeleteHistoryEvent = (PipelineBaseEvent) event;
            remoteDataCollector.deleteHistory(pipelineDeleteHistoryEvent.getUser(),
                pipelineDeleteHistoryEvent.getName(),
                pipelineDeleteHistoryEvent.getRev()
            );
            break;
          case DELETE_PIPELINE:
            PipelineBaseEvent pipelineDeleteEvent = (PipelineBaseEvent) event;
            remoteDataCollector.delete(pipelineDeleteEvent.getName(), pipelineDeleteEvent.getRev());
            break;
          case STOP_DELETE_PIPELINE:
            PipelineBaseEvent pipelineStopDeleteEvent = (PipelineBaseEvent) event;
            remoteDataCollector.stopAndDelete(pipelineStopDeleteEvent.getUser(),
                pipelineStopDeleteEvent.getName(),
                pipelineStopDeleteEvent.getRev()
            );
            break;
          case SYNC_ACL:
            remoteDataCollector.syncAcl(((SyncAclEvent) event).getAcl());
            break;
          case SSO_DISCONNECTED_MODE_CREDENTIALS:
            DisconnectedSecurityUtils.writeDisconnectedCredentials(
                disconnectedCredentialsDataStore,
                (DisconnectedSsoCredentialsEvent) event
            );
            break;
          default:
            ackEventMessage = Utils.format("Unrecognized event: '{}'", eventType);
            LOG.warn(ackEventMessage);
            break;
        }
      } catch (PipelineException | StageException ex) {
        ackEventMessage = Utils.format(
            "Remote event type: '{}' encountered exception '{}'",
            serverEvent.getEventType(),
            ex.getMessage()
        );
        LOG.warn(ackEventMessage, ex);
      } catch (IOException ex) {
        ackEventMessage = Utils.format("Remote event type: '{}' encountered exception while being deserialized '{}'",
            serverEvent.getEventType(),
            ex.getMessage()
        );
        LOG.warn(ackEventMessage, ex);
      }
      return ackEventMessage;
    }

    @Nullable
    private SourceOffset getSourceOffset(PipelineSaveEvent pipelineSaveEvent) throws IOException {
      String offset = pipelineSaveEvent.getOffset();
      SourceOffset sourceOffset;
      if (pipelineSaveEvent.getOffsetProtocolVersion() < 2) {
        // If the offset protocol version is less than 2, convert it to a structure similar to offset.json
        sourceOffset = new SourceOffset();
        sourceOffset.setOffset(offset);
      } else if (null == offset ) {
        // First run when offset is null
        sourceOffset = new SourceOffset(
            SourceOffset.CURRENT_VERSION,
            Collections.emptyMap()
        );
      } else {
        // Offset exists
        SourceOffsetJson sourceOffsetJson = ObjectMapperFactory.get().readValue(offset, SourceOffsetJson.class);
        sourceOffset = BeanHelper.unwrapSourceOffset(sourceOffsetJson);
      }
      new SourceOffsetUpgrader().upgrade(sourceOffset);
      return sourceOffset;
    }

    @VisibleForTesting
    ClientEvent handlePipelineEvent(ServerEventJson serverEventJson) {
      ServerEvent serverEvent = null;
      AckEventStatus ackEventStatus;
      String ackEventMessage;
      try {
        serverEvent = jsonToFromDto.asDto(serverEventJson);
        if (serverEvent != null) {
          ackEventMessage = handleServerEvent(serverEvent);
          ackEventStatus = ackEventMessage == null ? AckEventStatus.SUCCESS : AckEventStatus.ERROR;
        } else {
          ackEventStatus = AckEventStatus.IGNORE;
          ackEventMessage = Utils.format("Cannot understand remote event code {}", serverEventJson.getEventTypeId());
          LOG.warn(ackEventMessage);
        }
      } catch (IOException ex) {
        ackEventStatus = AckEventStatus.ERROR;
        if(serverEvent == null) {
          ackEventMessage = Utils.format("Can't parse event JSON", serverEventJson);
        } else {
          ackEventMessage = Utils.format(
            "Remote event type: '{}' encountered exception while being deserialized '{}'",
            serverEvent,
            ex.getMessage()
          );
        }
        LOG.warn(ackEventMessage, ex);
      }
      if (serverEventJson.isRequiresAck()) {
        AckEvent ackEvent = new AckEvent(ackEventStatus, ackEventMessage);
        return new ClientEvent(serverEventJson.getEventId(), jobEventDestinationList, false, true, EventType.ACK_EVENT,
          ackEvent, null);
      } else {
        return null;
      }
    }
  }

}
