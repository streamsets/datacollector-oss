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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.client.api.EventClient;
import com.streamsets.datacollector.event.client.api.EventException;
import com.streamsets.datacollector.event.client.impl.EventClientImpl;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.AckEventStatus;
import com.streamsets.datacollector.event.dto.BlobDeleteEvent;
import com.streamsets.datacollector.event.dto.BlobDeleteVersionEvent;
import com.streamsets.datacollector.event.dto.BlobStoreEvent;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.DisconnectedSsoCredentialsEvent;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelinePreviewEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvents;
import com.streamsets.datacollector.event.dto.PipelineStopAndDeleteEvent;
import com.streamsets.datacollector.event.dto.SDCBuildInfo;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.SDCProcessMetricsEvent;
import com.streamsets.datacollector.event.dto.SaveConfigurationEvent;
import com.streamsets.datacollector.event.dto.ServerEvent;
import com.streamsets.datacollector.event.dto.StageInfo;
import com.streamsets.datacollector.event.dto.SyncAclEvent;
import com.streamsets.datacollector.event.handler.DataCollector;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.StartPipelineContextBuilder;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.restapi.bean.StageOutputJson;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.production.SourceOffset;
import com.streamsets.datacollector.runner.production.SourceOffsetUpgrader;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.DisconnectedSecurityUtils;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.AbstractSSOService;
import com.streamsets.lib.security.http.DisconnectedSSOManager;
import com.streamsets.lib.security.http.DpmClientInfo;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.SSOConstants;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.streamsets.datacollector.event.handler.remote.RemoteDataCollector.DEFAULT_SEND_METRIC_ATTEMPTS;
import static com.streamsets.datacollector.event.handler.remote.RemoteDataCollector.SEND_METRIC_ATTEMPTS;


public class RemoteEventHandlerTask extends AbstractTask implements EventHandlerTask {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteEventHandlerTask.class);
  private static final long DEFAULT_PING_FREQUENCY = 5000;
  private static final long SYSTEM_LIMIT_MIN_PING_FREQUENCY = 5000;
  private static final long DEFAULT_STATUS_EVENTS_INTERVAL = 60000;
  // Default pipeline metrics interval is set to -1 because we are disabling this feature by default
  private static final long DEFAULT_PIPELINE_METRICS_INTERVAL = -1;
  private static final long SYSTEM_LIMIT_MIN_STATUS_EVENTS_INTERVAL = 30000;
  private static final String REMOTE_CONTROL = AbstractSSOService.CONFIG_PREFIX + "remote.control.";
  private static final long DEFAULT_PREVIEW_TIMEOUT = 10000L;

  public static final String REMOTE_JOB_LABELS = REMOTE_CONTROL + "job.labels";
  private static final String REMOTE_URL_PING_INTERVAL = REMOTE_CONTROL  + "ping.frequency";
  private static final String REMOTE_URL_SEND_ALL_STATUS_EVENTS_INTERVAL = REMOTE_CONTROL + "status.events.interval";
  private static final String REMOTE_URL_SEND_ALL_PIPELINE_METRICS_INTERVAL_MILLIS = REMOTE_CONTROL + "pipeline.metrics.interval.millis";
  private static final String DEFAULT_REMOTE_JOB_LABELS = "all";
  private static final String REMOTE_CONTROL_EVENTS_RECIPIENT = REMOTE_CONTROL + "events.recipient";
  private static final String DEFAULT_REMOTE_CONTROL_EVENTS_RECIPIENT = "jobrunner-app";
  private static final String REMOTE_CONTROL_PROCESS_EVENTS_RECIPIENTS = REMOTE_CONTROL + "process.events.recipients";
  private static final String REMOTE_URL_SYNC_EVENTS_PING_FREQUENCY = REMOTE_CONTROL  + "sync.events.ping.frequency";
  private static final String REMOTE_URL_HEARTBEAT_FREQUENCY = REMOTE_CONTROL  + "heartbeat.ping.frequency";
  private static final int DEFAULT_REMOTE_URL_HEARTBEAT_FREQUENCY = 60000;
  private static final int DEFAULT_REMOTE_URL_SYNC_EVENTS_PING_FREQUENCY = 5000;
  private static final String DEFAULT_REMOTE_CONTROL_PROCESS_EVENTS_RECIPIENTS = "jobrunner-app,timeseries-app";
  private static final String TIMESERIES_APP = "timeseries-app";
  private static final String REMOTE_URL_PIPELINE_STATUSES_ENDPOINT = "jobrunner/rest/v1/job/pipelineStatusEvents";
  private static final String REMOTE_URL_PIPELINE_STATUS_ENDPOINT = "jobrunner/rest/v1/job/pipelineStatusEvent";
  private static final String REMOTE_URL_SDC_PROCESS_METRICS_ENDPOINT = "jobrunner/rest/v1/sdc/processMetrics";
  private static final String REMOTE_URL_SDC_HEARTBEAT_ENDPOINT = "jobrunner/rest/v1/sdc/updateLastModifiedTime";
  public static final String OFFSET = "offset";
  public static final int OFFSET_PROTOCOL_VERSION = 2;
  private static final String DPM_JOB_ID = "dpm.job.id";
  private static final String JOB_ID = "JOB_ID";
  private final DataCollector remoteDataCollector;
  private final MessagingJsonToFromDto jsonToFromDto;
  private final SafeScheduledExecutorService executorService;
  private final SafeScheduledExecutorService syncEventsExecutorService;
  private final StageLibraryTask stageLibrary;
  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final List<String> appDestinationList;
  private final List<String> processAppDestinationList;
  private final List<String> labelList;
  private final Map<String, String> requestHeader;
  private final long defaultPingFrequency;
  private final long pipelineStatusSyncEventsSendFrequency;
  private final long heartbeatSendFrequency;
  private final Stopwatch stopWatch;
  private final Stopwatch stopWatchForSyncEvents;
  private final long sendAllStatusEventsInterval;
  private final DataStore dataStore;
  private final long sendAllPipelineMetricsInterval;
  private String jobRunnerMetricsUrl;
  private String messagingEventsUrl;
  private Configuration conf;
  private final int attempts;
  private String jobRunnerPipelineStatusEventsUrl;
  private String jobRunnerPipelineStatusEventUrl;
  private String jobRunnerSdcProcessMetricsEventUrl;
  private String jobRunnerSdcHeartBeatUrl;
  private boolean shouldSendSyncEvents;
  private int percentOfWaitIntervalBeforeSkip;
  // config to send via sync or async fashion till SDC-13109 is in
  public static final String SHOULD_SEND_SYNC_EVENTS = AbstractSSOService.CONFIG_PREFIX + "should.send.sync.events";
  private static final boolean SHOULD_SEND_SYNC_EVENTS_DEFAULT = false;
  // Percent of sendAllStatusEventsInterval after which sending events will be skipped
  public static final String PERCENT_OF_WAIT_INTERVAL_BEFORE_SKIP = AbstractSSOService.CONFIG_PREFIX +
      "percent.wait.interval.before.skip";
  public static final int DEFAULT_PERCENT_OF_WAIT_INTERVAL_BEFORE_SKIP = 70;
  private WebSocketToRestDispatcher webSocketToRestDispatcher;

  public RemoteEventHandlerTask(
      DataCollector remoteDataCollector,
      SafeScheduledExecutorService executorService,
      SafeScheduledExecutorService syncEventsExecutorService,
      StageLibraryTask stageLibrary,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration conf
  ) {
    this(
        remoteDataCollector,
        executorService,
        syncEventsExecutorService,
        stageLibrary,
        buildInfo,
        runtimeInfo,
        conf,
        null
    );
  }

  public RemoteEventHandlerTask(
      DataCollector remoteDataCollector,
      SafeScheduledExecutorService executorService,
      SafeScheduledExecutorService syncEventsExecutorService,
      StageLibraryTask stageLibrary,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration conf,
      DataStore disconnectedSsoCredentialsDataStore
  ) {
    super("REMOTE_EVENT_HANDLER");

    this.remoteDataCollector = remoteDataCollector;
    this.jsonToFromDto = MessagingJsonToFromDto.INSTANCE;
    this.executorService = executorService;
    this.syncEventsExecutorService = syncEventsExecutorService;
    this.stageLibrary = stageLibrary;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.conf = conf;
    this.attempts = conf.get(SEND_METRIC_ATTEMPTS, DEFAULT_SEND_METRIC_ATTEMPTS);
    this.shouldSendSyncEvents = conf.get(SHOULD_SEND_SYNC_EVENTS, SHOULD_SEND_SYNC_EVENTS_DEFAULT);
    appDestinationList = Arrays.asList(conf.get(
        REMOTE_CONTROL_EVENTS_RECIPIENT,
        DEFAULT_REMOTE_CONTROL_EVENTS_RECIPIENT
    ));
    String processAppsDest = conf.get(
        REMOTE_CONTROL_PROCESS_EVENTS_RECIPIENTS,
        DEFAULT_REMOTE_CONTROL_PROCESS_EVENTS_RECIPIENTS
    );
    processAppDestinationList = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(processAppsDest));
    String labels = conf.get(REMOTE_JOB_LABELS, DEFAULT_REMOTE_JOB_LABELS);
    labelList = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(labels));
    defaultPingFrequency = Math.max(conf.get(REMOTE_URL_PING_INTERVAL, DEFAULT_PING_FREQUENCY),
        SYSTEM_LIMIT_MIN_PING_FREQUENCY
    );
    pipelineStatusSyncEventsSendFrequency = Math.max(
        conf.get(REMOTE_URL_SYNC_EVENTS_PING_FREQUENCY, DEFAULT_REMOTE_URL_SYNC_EVENTS_PING_FREQUENCY),
        SYSTEM_LIMIT_MIN_PING_FREQUENCY
    );
    heartbeatSendFrequency = Math.max(conf.get(REMOTE_URL_HEARTBEAT_FREQUENCY, DEFAULT_REMOTE_URL_HEARTBEAT_FREQUENCY),
        SYSTEM_LIMIT_MIN_PING_FREQUENCY);

    sendAllStatusEventsInterval = Math.max(
        conf.get(REMOTE_URL_SEND_ALL_STATUS_EVENTS_INTERVAL, DEFAULT_STATUS_EVENTS_INTERVAL),
        SYSTEM_LIMIT_MIN_STATUS_EVENTS_INTERVAL
    );
    sendAllPipelineMetricsInterval = conf.get(REMOTE_URL_SEND_ALL_PIPELINE_METRICS_INTERVAL_MILLIS, DEFAULT_PIPELINE_METRICS_INTERVAL);
    percentOfWaitIntervalBeforeSkip = conf.get(PERCENT_OF_WAIT_INTERVAL_BEFORE_SKIP, DEFAULT_PERCENT_OF_WAIT_INTERVAL_BEFORE_SKIP);
    requestHeader = new HashMap<>();
    requestHeader.put(SSOConstants.X_REST_CALL, SSOConstants.SDC_COMPONENT_NAME);
    PipelineBeanCreator.prepareForConnections(conf, runtimeInfo);
    stopWatch = Stopwatch.createUnstarted();
    stopWatchForSyncEvents = Stopwatch.createUnstarted();

    File storeFile = new File(runtimeInfo.getDataDir(), DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_FILE);
    remoteDataCollector.init();
    if (disconnectedSsoCredentialsDataStore != null) {
      dataStore = disconnectedSsoCredentialsDataStore;
    } else {
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
    }
    jobRunnerMetricsUrl =  "jobrunner/rest/v1/jobs/metrics";
    messagingEventsUrl = RemoteDataCollector.MESSAGING_EVENTS_URL;
    jobRunnerPipelineStatusEventsUrl = REMOTE_URL_PIPELINE_STATUSES_ENDPOINT;
    jobRunnerPipelineStatusEventUrl = REMOTE_URL_PIPELINE_STATUS_ENDPOINT;
    jobRunnerSdcProcessMetricsEventUrl = REMOTE_URL_SDC_PROCESS_METRICS_ENDPOINT;
    jobRunnerSdcHeartBeatUrl = REMOTE_URL_SDC_HEARTBEAT_ENDPOINT;

    // Control Hub WebSocket Tunneling for Control Hub UI to Data Collector Communication
    webSocketToRestDispatcher = new WebSocketToRestDispatcher(conf, runtimeInfo, executorService);
  }

  @VisibleForTesting
  DataStore getDisconnectedSsoCredentialsDataStore() {
    return dataStore;
  }

  @VisibleForTesting
  String getJobRunnerPipelineStatusEventsUrl() {
    return jobRunnerPipelineStatusEventsUrl;
  }

  @VisibleForTesting
  String getJobRunnerPipelineStatusEventUrl() {
    return jobRunnerPipelineStatusEventUrl;
  }

  @VisibleForTesting
  String getSdcProcessMetricsEventUrl() {
    return jobRunnerSdcProcessMetricsEventUrl;
  }


  @Override
  public void runTask() {
    this.shouldSendSyncEvents = conf.get(SHOULD_SEND_SYNC_EVENTS, SHOULD_SEND_SYNC_EVENTS_DEFAULT);
    LOG.info("Will send sync events: {}", this.shouldSendSyncEvents);
    executorService.submit(new EventHandlerCallable(
        remoteDataCollector,
        new EventClientImpl(conf, () -> runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)),
        jsonToFromDto,
        new ArrayList<>(),
        new ArrayList<>(),
        getStartupReportEvent(),
        executorService,
        defaultPingFrequency,
        appDestinationList,
        processAppDestinationList,
        requestHeader,
        stopWatch,
        sendAllStatusEventsInterval,
        sendAllPipelineMetricsInterval,
        new LinkedHashMap<>(),
        runtimeInfo
    ));
    if (shouldSendSyncEvents) {
      HeartbeatSender heartbeatSender = new HeartbeatSender(new EventClientImpl(
          conf,
          () -> runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)
      ));
      syncEventsExecutorService.scheduleAtFixedRate(
          heartbeatSender,
          1000,
          heartbeatSendFrequency,
          TimeUnit.MILLISECONDS
      );
      syncEventsExecutorService.submit(new SyncEventSender(
          new EventClientImpl(conf, () -> runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)),
          remoteDataCollector,
          jsonToFromDto,
          sendAllStatusEventsInterval,
          runtimeInfo,
          syncEventsExecutorService,
          stopWatchForSyncEvents,
          pipelineStatusSyncEventsSendFrequency,
          percentOfWaitIntervalBeforeSkip
      ));
    }

    webSocketToRestDispatcher.runTask();
  }

  private ClientEvent getStartupReportEvent() {
    List<StageInfo> stageInfoList = new ArrayList<StageInfo>();
    for (StageDefinition stageDef : stageLibrary.getStages()) {
      stageInfoList.add(new StageInfo(stageDef.getName(), stageDef.getVersion(), stageDef.getLibrary()));
    }
    Runtime runtime = Runtime.getRuntime();
    SDCInfoEvent sdcInfoEvent = new SDCInfoEvent(
        runtimeInfo.getId(),
        runtimeInfo.getBaseHttpUrl(false),
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
        Strings.emptyToNull(runtimeInfo.getDeploymentId()),
        runtime.maxMemory()
    );
    return new ClientEvent(
        UUID.randomUUID().toString(),
        appDestinationList,
        false,
        false,
        EventType.SDC_INFO_EVENT,
        sdcInfoEvent,
        null
    );
  }

  @Override
  public void stopTask() {
    if (shouldSendSyncEvents) {
      EventClient eventClient = new EventClientImpl(conf,
          () -> runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY)
      );
      List<PipelineStatusEvent> pipelineStatusEventList = new ArrayList<>();
      try {
        for (PipelineAndValidationStatus pipelineAndValidationStatus : remoteDataCollector.getPipelines()) {
          pipelineStatusEventList.add(createPipelineStatusEvent(jsonToFromDto, pipelineAndValidationStatus));
        }
        PipelineStatusEvents pipelineStatusEvents = new PipelineStatusEvents();
        pipelineStatusEvents.setPipelineStatusEventList(pipelineStatusEventList);
        LOG.info("Sending status of all pipelines one last time in shutdown hook");
        eventClient.sendSyncEvents(jobRunnerPipelineStatusEventsUrl,
            new HashMap<>(),
            requestHeader,
            pipelineStatusEvents,
            1
        );
      } catch (Exception e) {
        LOG.error("Cannot send pipeline status events in shutdown hook: {}", e, e);
      }
    } else {
      LOG.info("Doing nothing as sending events in shutdown hook is not supported as sync events is set to false");
    }
    webSocketToRestDispatcher.stopTask();
    executorService.shutdownNow();
  }

  @Override
  public RemoteDataCollectorResult handleLocalEvent(
      Event event,
      EventType eventType,
      Map<String, ConnectionConfiguration> connections
  ) {
    RemoteDataCollectorResult result;
    try {
      switch (eventType) {
        case PREVIEW_PIPELINE:
          final PipelinePreviewEvent pipelinePreviewEvent = (PipelinePreviewEvent) event;

          // deserialize the JSON String representation of stage overrides in the preview event, into the JSON objects
          final List<StageOutputJson> stageOutputJsons = new LinkedList<>();
          final String stageOutputOverridesJsonText = pipelinePreviewEvent.getStageOutputsToOverrideJsonText();
          if (StringUtils.isNotBlank(stageOutputOverridesJsonText)) {
            final TypeReference<List<StageOutputJson>> typeRef = new TypeReference<List<StageOutputJson>>() {};
            stageOutputJsons.addAll(ObjectMapperFactory.get().readValue(
                stageOutputOverridesJsonText,
                typeRef
            ));
          }

          // convert the JSON objects into the DTO objects
          final List<StageOutput> stageOutputs = stageOutputJsons.stream().map(
              json -> json.getStageOutput()
          ).collect(Collectors.toList());
          long timeoutMillis = pipelinePreviewEvent.getTimeoutMillis() > 0 ?
              pipelinePreviewEvent.getTimeoutMillis() : DEFAULT_PREVIEW_TIMEOUT;
          result = RemoteDataCollectorResult.immediate(remoteDataCollector.previewPipeline(
              pipelinePreviewEvent.getUser(),
              pipelinePreviewEvent.getName(),
              pipelinePreviewEvent.getRev(),
              pipelinePreviewEvent.getBatches(),
              pipelinePreviewEvent.getBatchSize(),
              pipelinePreviewEvent.isSkipTargets(),
              pipelinePreviewEvent.isSkipLifecycleEvents(),
              pipelinePreviewEvent.getStopStage(),
              stageOutputs,
              timeoutMillis,
              pipelinePreviewEvent.isTestOrigin(),
              pipelinePreviewEvent.getInterceptorConfiguration(),
              pipelinePreviewEvent.getAfterActionsFunction(),
              connections
          ));
          break;
        default:
          result = handleEventHelper(event, eventType, connections);
          break;
      }
    } catch (Exception e) {
      LOG.error("Encountered exception handling local event type: '{}': {}", eventType, e.getMessage(), e);
      result = RemoteDataCollectorResult.error(Utils.format(
          "Local event type: '{}' encountered exception '{}'",
          eventType,
          e.getMessage()
      ));
    }
    return result;
  }

  @Override
  public RemoteDataCollectorResult handleRemoteEvent(Event event, EventType eventType, Map<String, ConnectionConfiguration> connections) {
    RemoteDataCollectorResult result;
    try {
      result = handleEventHelper(event, eventType, connections);
    } catch (Exception ex) {
      LOG.error("Encountered exception handling remote event type: '{}': {}", eventType, ex.getMessage(), ex);
      result = RemoteDataCollectorResult.error(Utils.format(
          "Remote event type: '{}' encountered exception '{}'",
          eventType,
          ex.getMessage()
      ));
    }
    return result;
  }

  private RemoteDataCollectorResult handleEventHelper(
      Event event,
      EventType eventType,
      Map<String, ConnectionConfiguration> connections
  ) throws Exception {
    RemoteDataCollectorResult result = RemoteDataCollectorResult.empty();
    switch (eventType) {
      case PING_FREQUENCY_ADJUSTMENT:
        result = RemoteDataCollectorResult.immediate(((PingFrequencyAdjustmentEvent) event).getPingFrequency());
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

        final String pipelineId = remoteDataCollector.savePipeline(pipelineSaveEvent.getUser(),
            pipelineSaveEvent.getName(),
            pipelineSaveEvent.getRev(),
            pipelineSaveEvent.getDescription(),
            sourceOffset,
            BeanHelper.unwrapPipelineConfiguration(pipelineConfigJson),
            BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson),
            pipelineSaveEvent.getAcl(),
            new HashMap<>(),
            connections
        );
        result = RemoteDataCollectorResult.immediate(pipelineId);
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
        PipelineStartEvent pipelineStartEvent = (PipelineStartEvent) event;
        Runner.StartPipelineContext startPipelineContext = new StartPipelineContextBuilder(pipelineStartEvent.getUser())
            .withInterceptorConfigurations(pipelineStartEvent.getInterceptorConfiguration())
            .build();
        Set<String> groups = null;
        if (pipelineStartEvent.getGroups() != null) {
          groups = new HashSet(pipelineStartEvent.getGroups());
        }
        remoteDataCollector.start(
            startPipelineContext,
            pipelineStartEvent.getName(),
            pipelineStartEvent.getRev(),
            groups
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
        remoteDataCollector.validateConfigs(
            pipelineValidataEvent.getUser(),
            pipelineValidataEvent.getName(),
            pipelineValidataEvent.getRev(),
            Collections.emptyList()
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
        PipelineStopAndDeleteEvent pipelineStopDeleteEvent = (PipelineStopAndDeleteEvent) event;
        result = RemoteDataCollectorResult.futureAck(remoteDataCollector.stopAndDelete(
            pipelineStopDeleteEvent.getUser(),
            pipelineStopDeleteEvent.getName(),
            pipelineStopDeleteEvent.getRev(),
            pipelineStopDeleteEvent.getForceTimeoutMillis()
        ));
        break;
      case BLOB_STORE:
        BlobStoreEvent blobStoreEvent = (BlobStoreEvent) event;
        remoteDataCollector.blobStore(
            blobStoreEvent.getNamespace(),
            blobStoreEvent.getId(),
            blobStoreEvent.getVersion(),
            blobStoreEvent.getContent()
        );
        break;
      case BLOB_DELETE:
        BlobDeleteEvent blobDeleteEvent = (BlobDeleteEvent) event;
        remoteDataCollector.blobDelete(
            blobDeleteEvent.getNamespace(),
            blobDeleteEvent.getId()
        );
        break;
      case BLOB_DELETE_VERSION:
        BlobDeleteVersionEvent blobDeleteVersionEvent = (BlobDeleteVersionEvent) event;
        remoteDataCollector.blobDelete(
            blobDeleteVersionEvent.getNamespace(),
            blobDeleteVersionEvent.getId(),
            blobDeleteVersionEvent.getVersion()
        );
        break;
      case SAVE_CONFIGURATION:
        SaveConfigurationEvent saveConfigurationEvent = (SaveConfigurationEvent)event;
        remoteDataCollector.storeConfiguration(saveConfigurationEvent.getConfiguration());
        break;
      case SYNC_ACL:
        remoteDataCollector.syncAcl(((SyncAclEvent) event).getAcl());
        break;
      case SSO_DISCONNECTED_MODE_CREDENTIALS:
        DisconnectedSecurityUtils.writeDisconnectedCredentials(
            getDisconnectedSsoCredentialsDataStore(),
            (DisconnectedSsoCredentialsEvent) event
        );
        break;
      default:
        result = RemoteDataCollectorResult.error(Utils.format("Unrecognized event: '{}'", eventType));
        break;
    }
    return result;
  }

  @Nullable
  private static SourceOffset getSourceOffset(PipelineSaveEvent pipelineSaveEvent) throws IOException {
    String offset = pipelineSaveEvent.getOffset();
    SourceOffset sourceOffset;
    if (pipelineSaveEvent.getOffsetProtocolVersion() < 2) {
      // If the offset protocol version is less than 2, convert it to a structure similar to offset.json
      sourceOffset = new SourceOffset();
      sourceOffset.setOffset(offset);
    } else if (null == offset) {
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

  class HeartbeatSender implements Runnable {
    private EventClient eventClient;

    public HeartbeatSender(
        EventClient eventClient
    ) {
      this.eventClient = eventClient;
    }

    @Override
    public void run() {
      try {
        callHeartBeatSender();
      } catch (Exception e) {
        LOG.warn("Cannot send heartbeat to SCH: {}", e, e);
      }
    }

    private void callHeartBeatSender() {
      LOG.debug("HeartBeat sender started");
      long startTime = System.currentTimeMillis();
      eventClient.sendSyncEvents(jobRunnerSdcHeartBeatUrl, new HashMap<>(), requestHeader, null, 1);
      long endTime = System.currentTimeMillis();
      LOG.debug("HeartBeat sender ended");
      logWarningIfAPICallTimeExceedsThreshold(jobRunnerSdcHeartBeatUrl, 30000, startTime, endTime);
    }
  }

  private static void logWarningIfAPICallTimeExceedsThreshold(String apiName, long threshold, long startTime, long endTime) {
    long duration = endTime - startTime;
    if (duration > threshold) {
      LOG.warn("Time taken to make {} call: {} secs", apiName, duration / 1000);
    }
  }

  class SyncEventSender implements Callable<Void> {
    private final DataCollector remoteDataCollector;
    private final MessagingJsonToFromDto jsonToFromDto;
    private final long waitBetweenSendingStatusEvents;
    private RuntimeInfo runtimeInfo;
    private final SafeScheduledExecutorService syncEventsExecutorService;
    private final EventClient eventClient;
    private final Stopwatch stopWatch;
    private long delay;
    private int percentOfWaitIntervalBeforeSkip;

    public SyncEventSender(
        EventClient eventClient,
        DataCollector remoteDataCollector,
        MessagingJsonToFromDto jsonToFromDto,
        long waitBetweenSendingStatusEvents,
        RuntimeInfo runtimeInfo,
        SafeScheduledExecutorService syncEventsExecutorService,
        Stopwatch stopWatch,
        long delay,
        int percentOfWaitIntervalBeforeSkip) {
      this.eventClient = eventClient;
      this.remoteDataCollector = remoteDataCollector;
      this.jsonToFromDto = jsonToFromDto;
      this.waitBetweenSendingStatusEvents = waitBetweenSendingStatusEvents;
      this.runtimeInfo = runtimeInfo;
      this.syncEventsExecutorService = syncEventsExecutorService;
      this.delay = delay;
      this.stopWatch = stopWatch;
      this.percentOfWaitIntervalBeforeSkip = percentOfWaitIntervalBeforeSkip;
    }

    @Override
    public Void call() {
      try {
        // This should be used by SDC > 3.12 to send critical messages to SCH directly.
        callSyncSender();
      } catch (Exception ex) {
        // only log a warning with error message to avoid filling up the logs in case SDC cannot connect to SCH
        LOG.warn("Cannot connect to send sync events: {}", ex.toString());
        LOG.trace("Entire error message", ex);
      } finally {
        syncEventsExecutorService.schedule(new SyncEventSender(
            eventClient,
            remoteDataCollector,
            jsonToFromDto,
            waitBetweenSendingStatusEvents,
            runtimeInfo,
            syncEventsExecutorService,
            stopWatch,
            delay,
            percentOfWaitIntervalBeforeSkip

        ), delay, TimeUnit.MILLISECONDS);
      } return null;
    }

    private void callSyncSender() {
      if (!stopWatch.isRunning() || stopWatch.elapsed(TimeUnit.MILLISECONDS) > waitBetweenSendingStatusEvents) {
        // get state of all running pipeline and state of all remote pipelines
        try {
          List<PipelineStatusEvent> pipelineStatusEventList = new ArrayList<>();

          stopWatch.reset();
          for (PipelineAndValidationStatus pipelineAndValidationStatus : remoteDataCollector.getPipelines()) {
            pipelineStatusEventList.add(createPipelineStatusEvent(jsonToFromDto, pipelineAndValidationStatus));
          }
          PipelineStatusEvents pipelineStatusEvents = new PipelineStatusEvents();
          pipelineStatusEvents.setPipelineStatusEventList(pipelineStatusEventList);
          LOG.debug("Sending status of all pipelines through sync thread");
          long startTime = System.currentTimeMillis();
          eventClient.sendSyncEvents(jobRunnerPipelineStatusEventsUrl,
              new HashMap<>(),
              requestHeader,
              pipelineStatusEvents,
              1
          );
          long endTime = System.currentTimeMillis();
          LOG.debug("Ended sending status of all pipelines through sync thread");
          logWarningIfAPICallTimeExceedsThreshold(jobRunnerPipelineStatusEventsUrl, 30000, startTime, endTime);
          if (hasElapsedWaitPercentInterval(stopWatch,
              waitBetweenSendingStatusEvents,
              percentOfWaitIntervalBeforeSkip
          )) {
            LOG.warn(
                "Timer already past {}% of wait interval, so not going to send process metrics event",
                percentOfWaitIntervalBeforeSkip
            );
          } else {
            LOG.debug("Sending process metrics event through sync thread");
            startTime = System.currentTimeMillis();
            eventClient.sendSyncEvents(jobRunnerSdcProcessMetricsEventUrl,
                new HashMap<>(),
                requestHeader,
                getSdcMetricsEvent(runtimeInfo),
                1
            );
            endTime = System.currentTimeMillis();
            LOG.debug("Ended sending process metrics event through sync thread");
            logWarningIfAPICallTimeExceedsThreshold(jobRunnerSdcProcessMetricsEventUrl, 30000, startTime, endTime);
          }
        } catch (Exception e) {
          LOG.warn(Utils.format("Error while sending pipeline updates to DPM: '{}'", e), e);
        }
      } else {
        // get state of only remote pipelines which changed state
        try {
          List<PipelineAndValidationStatus> pipelineAndValidationStatuses = remoteDataCollector.getRemotePipelinesWithChanges();
          for (PipelineAndValidationStatus pipelineAndValidationStatus : pipelineAndValidationStatuses) {
            PipelineStatusEvent pipelineStatusEvent = createPipelineStatusEvent(jsonToFromDto,
                pipelineAndValidationStatus
            );
            LOG.debug(Utils.format(
                "Sending event for remote pipeline: '{}' in status: '{}' through sync thread",
                pipelineStatusEvent.getName(),
                pipelineStatusEvent.getPipelineStatus()
            ));
            long startTime = System.currentTimeMillis();
            eventClient.sendSyncEvents(jobRunnerPipelineStatusEventUrl,
                new HashMap<>(),
                requestHeader,
                pipelineStatusEvent,
                1
            );
            long endTime = System.currentTimeMillis();
            LOG.debug(Utils.format(
                "Ended sending event for remote pipeline: '{}' in status: '{}' through sync thread",
                pipelineStatusEvent.getName(),
                pipelineStatusEvent.getPipelineStatus()
            ));
            logWarningIfAPICallTimeExceedsThreshold(jobRunnerPipelineStatusEventUrl, 30000, startTime, endTime);
          }
        } catch (Exception e) {
          LOG.error(Utils.format("Error while sending a single pipeline status to DPM: {}", e), e);
        }
      }
      if (!stopWatch.isRunning()) {
        stopWatch.start();
      }
    }
  }

  static boolean hasElapsedWaitPercentInterval(
      Stopwatch stopWatch, long waitBetweenSendingStatusEvents, int x
  ) {
    if (stopWatch.elapsed(TimeUnit.MILLISECONDS) >= (float) x / 100 * waitBetweenSendingStatusEvents) {
      return true;
    } else {
      return false;
    }
  }

  static PipelineStatusEvent createPipelineStatusEvent(
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
    LOG.debug(
        "Created pipeline status event with name and title {}::{}",
        pipelineAndValidationStatus.getName(),
        pipelineAndValidationStatus.getTitle()
    );
    return pipelineStatusEvent;
  }

  @VisibleForTesting
  class EventHandlerCallable implements Callable<Void> {
    private final DataCollector remoteDataCollector;
    private final EventClient eventClient;
    private final MessagingJsonToFromDto jsonToFromDto;
    private final SafeScheduledExecutorService executorService;
    private final Map<String, String> requestHeader;
    private final List<String> jobEventDestinationList;
    private final List<String> processAppDestinationList;
    private final Stopwatch stopWatch;
    private final long waitBetweenSendingStatusEvents;
    private final long waitBetweenSendingPipelineMetrics;
    private List<ClientEvent> ackEventList;
    private List<ClientEvent> remoteEventList;
    private ClientEvent sdcInfoEvent;
    private long delay;
    private Map<ServerEvent, Future<AckEvent>> eventToAckEventFuture;
    private RuntimeInfo runtimeInfo;

    public EventHandlerCallable(
        DataCollector remoteDataCollector,
        EventClient messagingEventClient,
        MessagingJsonToFromDto jsonToFromDto,
        List<ClientEvent> ackEventList,
        List<ClientEvent> remoteEventList,
        ClientEvent sdcInfoEvent,
        SafeScheduledExecutorService executorService,
        long delay,
        List<String> jobEventDestinationList,
        List<String> processAppDestinationList,
        Map<String, String> requestHeader,
        Stopwatch stopWatch,
        long waitBetweenSendingStatusEvents,
        long waitBetweenSendingPipelineMetrics,
        Map<ServerEvent, Future<AckEvent>> eventToAckEventFuture,
        RuntimeInfo runtimeInfo
    ) {
      this.remoteDataCollector = remoteDataCollector;
      this.eventClient = messagingEventClient;
      this.jsonToFromDto = jsonToFromDto;
      this.executorService = executorService;
      this.delay = delay;
      this.jobEventDestinationList = jobEventDestinationList;
      this.processAppDestinationList = processAppDestinationList;
      this.ackEventList = ackEventList;
      this.remoteEventList = remoteEventList;
      this.sdcInfoEvent = sdcInfoEvent;
      this.requestHeader = requestHeader;
      this.stopWatch = stopWatch;
      this.waitBetweenSendingStatusEvents = waitBetweenSendingStatusEvents;
      this.waitBetweenSendingPipelineMetrics = waitBetweenSendingPipelineMetrics;
      this.eventToAckEventFuture = eventToAckEventFuture;
      this.runtimeInfo = runtimeInfo;
    }

    @Override
    public Void call() {
      try {
        callRemoteControl();
      } catch (Exception ex) {
        // only log a warning with error message to avoid filling up the logs in case SDC cannot connect to SCH
        LOG.warn("Cannot connect to send/receive events: {}", ex.toString());
        LOG.trace("Entire error message", ex);
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
            processAppDestinationList,
            requestHeader,
            stopWatch,
            waitBetweenSendingStatusEvents,
            waitBetweenSendingPipelineMetrics,
            eventToAckEventFuture,
            runtimeInfo
        ), delay, TimeUnit.MILLISECONDS);
      }
      return null;
    }

    @VisibleForTesting
    long getDelay() {
      return this.delay;
    }

    @VisibleForTesting
    List<ClientEvent> getAckEventList() {
      return ackEventList;
    }

    private void sendEventsAsync(List<ClientEvent> clientEventList) {
      try {
        if (!stopWatch.isRunning() || stopWatch.elapsed(TimeUnit.MILLISECONDS) > waitBetweenSendingStatusEvents) {
          // get state of all running pipeline and state of all remote pipelines
          List<PipelineStatusEvent> pipelineStatusEventList = new ArrayList<>();
          LOG.debug("Sending status of all pipelines in async fashion");
          for (PipelineAndValidationStatus pipelineAndValidationStatus : remoteDataCollector.getPipelines()) {
            pipelineStatusEventList.add(createPipelineStatusEvent(jsonToFromDto, pipelineAndValidationStatus));
          }
          stopWatch.reset();
          PipelineStatusEvents pipelineStatusEvents = new PipelineStatusEvents();
          pipelineStatusEvents.setPipelineStatusEventList(pipelineStatusEventList);
          clientEventList.add(new ClientEvent
              (UUID.randomUUID().toString(),
                  jobEventDestinationList,
                  false,
                  false,
                  EventType.STATUS_MULTIPLE_PIPELINES,
                  pipelineStatusEvents,
                  null
              ));
          // Clear this state change list as we are fetching all events
          remoteEventList.clear();

          // Add SDC Metrics Event
          clientEventList.add(new ClientEvent(
              UUID.randomUUID().toString(),
              processAppDestinationList,
              false,
              false,
              EventType.SDC_PROCESS_METRICS_EVENT,
              getSdcMetricsEvent(runtimeInfo),
              null
          ));
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
            LOG.info(Utils.format("Sending event for remote pipeline: '{}' in status: '{}'",
                pipelineStatusEvent.getName(), pipelineStatusEvent.getPipelineStatus()));
          }
        }
      } catch (Exception ex) {
        LOG.warn(Utils.format("Error while creating/serializing pipeline status event: '{}'", ex), ex);
      }
      clientEventList.addAll(remoteEventList);
    }

    @VisibleForTesting
    void callRemoteControl() {
      List<ClientEvent> clientEventList = new ArrayList<>();
      for (ClientEvent ackEvent : ackEventList) {
        clientEventList.add(ackEvent);
      }
      clientEventList.addAll(getQueuedAckEvents());
      if (sdcInfoEvent != null) {
        clientEventList.add(sdcInfoEvent);
      }

      if (!shouldSendSyncEvents) {
        sendEventsAsync(clientEventList);
      } else {
        // sends async message to timeseries-app even if sync events is turned on
        if (!stopWatch.isRunning() || stopWatch.elapsed(TimeUnit.MILLISECONDS) > waitBetweenSendingStatusEvents) {
          stopWatch.reset();
          LOG.debug("Sending metrics event to time-series app");
          clientEventList.add(new ClientEvent(
              UUID.randomUUID().toString(),
              Collections.singletonList(TIMESERIES_APP),
              false,
              false,
              EventType.SDC_PROCESS_METRICS_EVENT,
              getSdcMetricsEvent(runtimeInfo),
              null
          ));
        }
      }
      List<ServerEventJson> serverEventJsonList;
      try {
        // If waitBetweenSendingPipelineMetrics is set to -1, pipeline metrics sending is disabled
        if (waitBetweenSendingPipelineMetrics != -1 && (!stopWatch.isRunning() || stopWatch.elapsed(TimeUnit.MILLISECONDS) > waitBetweenSendingPipelineMetrics)) {
          // We're currently sending with attempts set to 0 because we are disabling this function by default
          sendPipelineMetrics(remoteDataCollector, eventClient, jobRunnerMetricsUrl, requestHeader, attempts);
        }
      } catch (IOException | PipelineException e) {
        LOG.warn("Error while sending metrics to server:  " + e, e);
      }
      try {
        List<ClientEventJson> clientEventJsonList = jsonToFromDto.toJson(clientEventList);
        serverEventJsonList = eventClient.submit(messagingEventsUrl, new HashMap<>(), requestHeader, false, clientEventJsonList);
        remoteEventList.clear();
        if (!eventToAckEventFuture.isEmpty()) {
          Set<String> eventIds = clientEventList.stream().map(ClientEvent::getEventId).collect(Collectors.toSet());
          Set<ServerEvent> eventsAlreadyAcked = eventToAckEventFuture.keySet().stream().filter(serverEvent ->
              eventIds.contains(
                  serverEvent.getEventId())).collect(Collectors.toSet());
          LOG.info("Removing already acked events {}", eventsAlreadyAcked);
          eventToAckEventFuture.keySet().removeAll(eventsAlreadyAcked);
        }
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
      Event event = serverEvent.getEvent();
      EventType eventType = serverEvent.getEventType();
      LOG.info("Handling {} event: '{}' ", eventType, serverEvent);
      RemoteDataCollectorResult result = handleRemoteEvent(event, eventType, new HashMap<>());
      if (result.isError()) {
        LOG.error(result.getErrorMessage());
        ackEventMessage = result.getErrorMessage();
      } else {
        if (result.getFutureAck() != null) {
          eventToAckEventFuture.put(serverEvent, result.getFutureAck());
        }
        if (result.getImmediateResult() != null) {
          switch (eventType) {
            case PING_FREQUENCY_ADJUSTMENT:
              delay = (long) result.getImmediateResult();
              break;
            default:
              // we don't need the immediate result, so just log for now
              if (LOG.isDebugEnabled()) {
                LOG.debug("Immediate result from handling remote {} event: {}", eventType, result.getImmediateResult());
              }
              break;
          }
        }
      }
      return ackEventMessage;
    }

    private List<ClientEvent> getQueuedAckEvents() {
      List<ClientEvent> clientEvents = new ArrayList<>();
      eventToAckEventFuture.entrySet().forEach(eventIdToAckEventFuture -> {
        Future<AckEvent> future = eventIdToAckEventFuture.getValue();
        if (future.isDone()) {
          ServerEvent serverEvent = eventIdToAckEventFuture.getKey();
          AckEvent ackEvent;
          try {
            ackEvent = future.get();
          } catch (Exception e) {
            String errorMsg = Utils.format("Error while trying to get an ack event for eventType {}, eventId: {}, error is {} ", serverEvent
                    .getEventType(),
                serverEvent.getEventId(),
                e);
            LOG.warn(errorMsg, e);
            ackEvent = new AckEvent(AckEventStatus.ERROR, errorMsg);
          }
          clientEvents.add(new ClientEvent(serverEvent.getEventId(),
              jobEventDestinationList,
              false,
              true,
              EventType.ACK_EVENT,
              ackEvent,
              null
          ));
        }
      });
      return clientEvents;
    }

    @VisibleForTesting
    ClientEvent handlePipelineEvent(ServerEventJson serverEventJson) {
      Set<String> eventIdSet = eventToAckEventFuture.keySet().stream().map(ServerEvent::getEventId).collect
          (Collectors.toSet());
      if (eventIdSet.contains(serverEventJson.getEventId())) {
        LOG.debug("Not processing event {} of type {} as its already being processed",
            serverEventJson.getEventId(),
            serverEventJson.getEventTypeId()
        );
        return null;
      }
      ServerEvent serverEvent = null;
      AckEventStatus ackEventStatus;
      String ackEventMessage;
      try {
        serverEvent = jsonToFromDto.asDto(serverEventJson);
        if (serverEvent != null) {
          ackEventMessage = handleServerEvent(serverEvent);
          if (serverEvent.getEventType() == EventType.STOP_DELETE_PIPELINE) {
            // no sync ack with stop and delete pipeline event;
            return null;
          }
          ackEventStatus = ackEventMessage == null ? AckEventStatus.SUCCESS : AckEventStatus.ERROR;
        } else {
          ackEventStatus = AckEventStatus.IGNORE;
          ackEventMessage = Utils.format("Cannot understand remote event code {}", serverEventJson.getEventTypeId());
          LOG.warn(ackEventMessage);
        }
      } catch (IOException ex) {
        ackEventStatus = AckEventStatus.ERROR;
        if (serverEvent == null) {
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

  static SDCProcessMetricsEvent getSdcMetricsEvent(RuntimeInfo runtimeInfo) {
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    Runtime runtime = Runtime.getRuntime();
    SDCProcessMetricsEvent sdcProcessMetricsEvent = new SDCProcessMetricsEvent();
    sdcProcessMetricsEvent.setTimestamp(System.currentTimeMillis());
    sdcProcessMetricsEvent.setSdcId(runtimeInfo.getId());
    sdcProcessMetricsEvent.setCpuLoad(osBean.getProcessCpuLoad() * 100);
    sdcProcessMetricsEvent.setUsedMemory(runtime.totalMemory() - runtime.freeMemory());
    return sdcProcessMetricsEvent;
  }

  static void sendPipelineMetrics(
      DataCollector remoteDataCollector,
      EventClient eventClient,
      String jobRunnerUrl,
      Map<String, String> requestHeader,
      long attempts
  ) throws IOException, PipelineException {
    if (attempts == 0) {
      // avoid the below deserialization if never planning to send metrics
      return;
    }
    ObjectMapper objectMapper = ObjectMapperFactory.get();
    List<SDCMetricsJson> sdcMetricsJsonList = new ArrayList<>();
    for (PipelineState pipelineState : remoteDataCollector.getRemotePipelines()) {
      Runner runner = remoteDataCollector.getRunner(pipelineState.getPipelineId(), pipelineState.getRev());
      if (runner != null) {
        PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get()
            .create(runner.getPipelineConfiguration(pipelineState.getUser()), new ArrayList<>(), null, null, null);
        SDCMetricsJson sdcMetricsJson = new SDCMetricsJson();

        Object metrics = runner.getMetrics();
        if (metrics instanceof MetricRegistry) {
          MetricRegistry metricRegistry = (MetricRegistry) metrics ;
          sdcMetricsJson.setMetrics(ObjectMapperFactory.get()
              .readValue(objectMapper.writer().writeValueAsString(metricRegistry), MetricRegistryJson.class));
        } else if (metrics instanceof MetricRegistryJson) {
          sdcMetricsJson.setMetrics((MetricRegistryJson) metrics);
        }

        Map<String, String> metadata = new HashMap<>();
        metadata.put(DPM_JOB_ID, (String) pipelineConfigBean.constants.get(JOB_ID));
        sdcMetricsJson.setMetadata(metadata);
        sdcMetricsJsonList.add(sdcMetricsJson);
      }
    }
    if (sdcMetricsJsonList.size() != 0) {
      // We're currently sending with attempts set to 0 because we are disabling this function by default
      eventClient.submit(jobRunnerUrl, new HashMap<>(), requestHeader, sdcMetricsJsonList, attempts);
    }
  }

}
