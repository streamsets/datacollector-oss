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
package com.streamsets.datacollector.event.binding;

import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.BlobDeleteEvent;
import com.streamsets.datacollector.event.dto.BlobDeleteVersionEvent;
import com.streamsets.datacollector.event.dto.BlobStoreEvent;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.DisconnectedSsoCredentialsEvent;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineDeleteEvent;
import com.streamsets.datacollector.event.dto.PipelineHistoryDeleteEvent;
import com.streamsets.datacollector.event.dto.PipelinePreviewEvent;
import com.streamsets.datacollector.event.dto.PipelineResetEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvents;
import com.streamsets.datacollector.event.dto.PipelineStopAndDeleteEvent;
import com.streamsets.datacollector.event.dto.PipelineStopEvent;
import com.streamsets.datacollector.event.dto.PipelineValidateEvent;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.SDCProcessMetricsEvent;
import com.streamsets.datacollector.event.dto.SaveConfigurationEvent;
import com.streamsets.datacollector.event.dto.ServerEvent;
import com.streamsets.datacollector.event.dto.SyncAclEvent;
import com.streamsets.datacollector.event.json.AckEventJson;
import com.streamsets.datacollector.event.json.BlobDeleteEventJson;
import com.streamsets.datacollector.event.json.BlobDeleteVersionEventJson;
import com.streamsets.datacollector.event.json.BlobStoreEventJson;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.DisconnectedSsoCredentialsEventJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineDeleteEventJson;
import com.streamsets.datacollector.event.json.PipelineHistoryDeleteEventJson;
import com.streamsets.datacollector.event.json.PipelinePreviewEventJson;
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
import com.streamsets.datacollector.event.json.SDCProcessMetricsEventJson;
import com.streamsets.datacollector.event.json.SaveConfigurationEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;
import com.streamsets.datacollector.event.json.SyncAclEventJson;
import com.streamsets.lib.security.http.DisconnectedSecurityInfo;
import fr.xebia.extras.selma.Mapper;
import fr.xebia.extras.selma.Maps;
import fr.xebia.extras.selma.Selma;

@Mapper
public abstract class MessagingDtoJsonMapper {
  public static final MessagingDtoJsonMapper INSTANCE = Selma.builder(MessagingDtoJsonMapper.class).build();

  public abstract PipelineSaveEventJson toPipelineSaveEventJson(PipelineSaveEvent pipelineSaveEvent);

  public abstract PipelineSaveEvent asPipelineSaveEventDto(PipelineSaveEventJson pipelineSaveEventJson);

  public abstract PipelineSaveRulesEventJson toPipelineSaveRulesEventJson(PipelineSaveRulesEvent pipelineSaveRulesEvent);

  public abstract PipelineSaveRulesEvent asPipelineSaveRulesEventDto(PipelineSaveRulesEventJson pipelineSaveRulesEventJson);

  public abstract PipelineStopAndDeleteEventJson toPipelineStopAndDeleteEventJson(PipelineStopAndDeleteEvent event);

  public abstract PipelineStartEvent asPipelineStartEventDto(PipelineStartEventJson pipelineActionEventJson);

  public abstract PipelineStartEventJson toPipelineStartEventJson(PipelineStartEvent event);

  public abstract PipelineStartEvent.InterceptorConfiguration asInterceptorConfigurationDto(
    PipelineStartEventJson.InterceptorConfigurationJson json
  );

  public abstract PipelineStartEventJson.InterceptorConfigurationJson toInterceptorConfigurationJson(
    PipelineStartEvent.InterceptorConfiguration conf
  );

  public abstract PipelineStopAndDeleteEvent asPipelineStopAndDeleteEventDto(
      PipelineStopAndDeleteEventJson pipelineStopAndDeleteEventJson
  );

  public abstract DisconnectedSsoCredentialsEvent.Entry asDisconectedSsoCredentialsDto(
      DisconnectedSsoCredentialsEventJson.EntryJson json
  );

  public abstract DisconnectedSsoCredentialsEvent asDisconectedSsoCredentialsDto(DisconnectedSsoCredentialsEventJson
      json);

  public abstract DisconnectedSsoCredentialsEventJson.EntryJson toDisconectedSsoCredentialsJson(
      DisconnectedSsoCredentialsEvent.Entry json
  );

  public abstract DisconnectedSsoCredentialsEventJson toDisconectedSsoCredentialsJson(
      DisconnectedSsoCredentialsEvent json
  );

  public abstract PingFrequencyAdjustmentEventJson toPingFrequencyAdjustmentEventJson(PingFrequencyAdjustmentEvent pingFrequencyEvent);

  public abstract PingFrequencyAdjustmentEvent asPingFrequencyAdjustmentEventDto(PingFrequencyAdjustmentEventJson pingFrequencyEventJson);

  public abstract AckEventJson toAckEventJson(AckEvent ackEvent);

  public abstract AckEvent asAckEventDto(AckEventJson ackEvent);

  public abstract PipelineStatusEventJson toPipelineStatusEventJson(PipelineStatusEvent pipelineStatusEvent);

  public abstract PipelineStatusEvent asPipelineStatusEventDto(PipelineStatusEventJson pipelineStatusEvent);

  public abstract PipelineStatusEvents asPipelineStatusEventsDto(PipelineStatusEventsJson pipelineStatusEvent);

  public abstract PipelineStatusEventsJson toPipelineStatusEventsJson(PipelineStatusEvents pipelineStatusEvents);

  public abstract SDCInfoEvent asSDCInfoEventDto(SDCInfoEventJson sdcInfoEventJson);

  public abstract SDCInfoEventJson toSDCInfoEventJson(SDCInfoEvent sdcInfoEvent);

  public abstract SDCProcessMetricsEvent asSDCMetricsEventDto(SDCProcessMetricsEventJson sdcProcessMetricsEventJson);

  public abstract SDCProcessMetricsEventJson toSDCMetricsEventJson(SDCProcessMetricsEvent sdcProcessMetricsEvent);

  public abstract SyncAclEventJson toSyncAclEventJson(SyncAclEvent syncAclEvent);

  public abstract DisconnectedSsoCredentialsEvent toJson(DisconnectedSecurityInfo info);

  public abstract SyncAclEvent asSyncAclEventDto(SyncAclEventJson syncAclEvent);

  @Maps(withIgnoreFields = {"payload", "event"})
  public abstract ServerEvent asServerEventDto(ServerEventJson serverEventJson);

  @Maps(withIgnoreFields = {"payload", "event", "eventType"})
  public abstract ClientEventJson toClientEventJson(ClientEvent clientEvent);

  public abstract BlobStoreEvent asBlobStoreEventDto(BlobStoreEventJson blobStoreEventJson);

  public abstract BlobStoreEventJson toBlobStoreEventJson(BlobStoreEvent blobStoreEvent);

  public abstract BlobDeleteEvent asBlobDeleteEventDto(BlobDeleteEventJson blobDeleteBaseEventJson);

  public abstract BlobDeleteEventJson toBlobDeleteEventJson(BlobDeleteEvent blobDeleteBaseEvent);

  public abstract BlobDeleteVersionEvent asBlobDeleteVersionEventDto(BlobDeleteVersionEventJson blobDeleteEventJson);

  public abstract BlobDeleteVersionEventJson toBlobDeleteVersionEventJson(BlobDeleteVersionEvent blobDeleteEvent);

  public abstract SaveConfigurationEvent asSaveConfigurationEventDto(SaveConfigurationEventJson saveConfigurationEventJson);

  public abstract SaveConfigurationEventJson toSaveConfigurationEventJson(SaveConfigurationEvent saveConfigurationEvent);

  @Maps(withIgnoreFields = "afterActionsFunction")
  public abstract PipelinePreviewEvent asPipelinePreviewEventDto(PipelinePreviewEventJson pipelineActionEventJson);

  @Maps(withIgnoreFields = "afterActionsFunction")
  public abstract PipelinePreviewEventJson toPipelinePreviewEventJson(PipelinePreviewEvent event);

  public abstract PipelineStopEvent asPipelineStopEventDto(PipelineStopEventJson pipelineStopEventJson);
  public abstract PipelineStopEventJson toPipelineStopEventJson(PipelineStopEvent pipelineStopEvent);

  public abstract PipelineDeleteEvent asPipelineDeleteEventDto(PipelineDeleteEventJson pipelineDeleteEventJson);
  public abstract PipelineDeleteEventJson toPipelineDeleteEventJson(PipelineDeleteEvent pipelineDeleteEvent);

  public abstract PipelineHistoryDeleteEvent asPipelineHistoryDeleteEventDto(
      PipelineHistoryDeleteEventJson pipelineHistoryDeleteEventJson
  );
  public abstract PipelineHistoryDeleteEventJson toPipelineHistoryDeleteEventJson(
      PipelineHistoryDeleteEvent pipelineHistoryDeleteEvent
  );

  public abstract PipelineValidateEvent asPipelineValidateEventDto(PipelineValidateEventJson pipelineValidateEventJson);
  public abstract PipelineValidateEventJson toPipelineValidateEventJson(PipelineValidateEvent pipelineValidateEvent);

  public abstract PipelineResetEvent asPipelineResetEventDto(PipelineResetEventJson pipelineResetEventJson);
  public abstract PipelineResetEventJson toPipelineResetEventJson(PipelineResetEvent pipelineResetEvent);

}
