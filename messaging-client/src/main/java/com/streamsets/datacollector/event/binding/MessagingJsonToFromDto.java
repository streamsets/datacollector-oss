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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.BlobDeleteEvent;
import com.streamsets.datacollector.event.dto.BlobDeleteVersionEvent;
import com.streamsets.datacollector.event.dto.BlobStoreEvent;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.DisconnectedSsoCredentialsEvent;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
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
import com.streamsets.datacollector.event.json.EventJson;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MessagingJsonToFromDto {

  public static final MessagingJsonToFromDto INSTANCE = getInstance();
  private static ObjectMapper mapper;

  private MessagingJsonToFromDto() {
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
  }

  private static MessagingJsonToFromDto getInstance() {
    return new MessagingJsonToFromDto();
  }

  private static final Logger LOG = LoggerFactory.getLogger(MessagingJsonToFromDto.class);

  public <T> T deserialize(String body, TypeReference<T> type) throws IOException {
    return mapper.readValue(body, type);
  }

  public <T> T deserialize(String body, Class<T> type) throws IOException {
    return mapper.readValue(body, type);
  }

  public String serialize(Object object) throws JsonProcessingException {
    return mapper.writeValueAsString(object);
  }

  public Event asDto(EventJson eventJson, int eventTypeId) throws IOException {
    return asDto(eventJson, EventType.fromValue(eventTypeId));
  }

  private interface EventJsonSupplier {
    EventJson supplyJson(Class<? extends EventJson> jsonClass) throws IOException;
  }

  public Event toDtoHelper(EventType eventType, EventJsonSupplier eventJsonSupplierImpl) throws IOException {
    final MessagingDtoJsonMapper inst = MessagingDtoJsonMapper.INSTANCE;
    switch (eventType) {
      case PING_FREQUENCY_ADJUSTMENT:
        return inst.asPingFrequencyAdjustmentEventDto((PingFrequencyAdjustmentEventJson) eventJsonSupplierImpl.supplyJson(
            PingFrequencyAdjustmentEventJson.class
        ));
      case SAVE_PIPELINE:
        return inst.asPipelineSaveEventDto((PipelineSaveEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineSaveEventJson.class
        ));
      case SAVE_RULES_PIPELINE:
        return inst.asPipelineSaveRulesEventDto((PipelineSaveRulesEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineSaveRulesEventJson.class
        ));
      case STATUS_PIPELINE:
        return inst.asPipelineStatusEventDto((PipelineStatusEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineStatusEventJson.class
        ));
      case STATUS_MULTIPLE_PIPELINES:
        return inst.asPipelineStatusEventsDto((PipelineStatusEventsJson) eventJsonSupplierImpl.supplyJson(
            PipelineStatusEventsJson.class
        ));
      case ACK_EVENT:
        return inst.asAckEventDto((AckEventJson) eventJsonSupplierImpl.supplyJson(
            AckEventJson.class
        ));
      case SDC_INFO_EVENT:
        return inst.asSDCInfoEventDto((SDCInfoEventJson) eventJsonSupplierImpl.supplyJson(
            SDCInfoEventJson.class
        ));
      case SDC_PROCESS_METRICS_EVENT:
        return inst.asSDCMetricsEventDto((SDCProcessMetricsEventJson) eventJsonSupplierImpl.supplyJson(
            SDCProcessMetricsEventJson.class
        ));
      case SYNC_ACL:
        return inst.asSyncAclEventDto((SyncAclEventJson) eventJsonSupplierImpl.supplyJson(
            SyncAclEventJson.class
        ));
      case STOP_DELETE_PIPELINE:
        return inst.asPipelineStopAndDeleteEventDto((PipelineStopAndDeleteEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineStopAndDeleteEventJson.class
        ));
      case START_PIPELINE:
        return inst.asPipelineStartEventDto((PipelineStartEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineStartEventJson.class
        ));
      case PREVIEW_PIPELINE:
        return inst.asPipelinePreviewEventDto((PipelinePreviewEventJson) eventJsonSupplierImpl.supplyJson(
            PipelinePreviewEventJson.class
        ));
      case STOP_PIPELINE:
        return inst.asPipelineStopEventDto((PipelineStopEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineStopEventJson.class
        ));
      case VALIDATE_PIPELINE:
        return inst.asPipelineValidateEventDto((PipelineValidateEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineValidateEventJson.class
        ));
      case RESET_OFFSET_PIPELINE:
        return inst.asPipelineResetEventDto((PipelineResetEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineResetEventJson.class
        ));
      case DELETE_HISTORY_PIPELINE:
        return inst.asPipelineHistoryDeleteEventDto((PipelineHistoryDeleteEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineHistoryDeleteEventJson.class
        ));
      case DELETE_PIPELINE:
        return inst.asPipelineDeleteEventDto((PipelineDeleteEventJson) eventJsonSupplierImpl.supplyJson(
            PipelineDeleteEventJson.class
        ));
      case BLOB_STORE:
        return inst.asBlobStoreEventDto((BlobStoreEventJson) eventJsonSupplierImpl.supplyJson(
            BlobStoreEventJson.class
        ));
      case BLOB_DELETE:
        return inst.asBlobDeleteEventDto((BlobDeleteEventJson) eventJsonSupplierImpl.supplyJson(
            BlobDeleteEventJson.class
        ));
      case BLOB_DELETE_VERSION:
        return inst.asBlobDeleteVersionEventDto((BlobDeleteVersionEventJson) eventJsonSupplierImpl.supplyJson(
            BlobDeleteVersionEventJson.class
        ));
      case SAVE_CONFIGURATION:
        return inst.asSaveConfigurationEventDto((SaveConfigurationEventJson) eventJsonSupplierImpl.supplyJson(
            SaveConfigurationEventJson.class
        ));
      case SSO_DISCONNECTED_MODE_CREDENTIALS:
        return inst.asDisconectedSsoCredentialsDto((DisconnectedSsoCredentialsEventJson) eventJsonSupplierImpl.supplyJson(
            DisconnectedSsoCredentialsEventJson.class
        ));
      default:
        throw new IllegalStateException("Unrecognized event type: " + eventType);
    }
  }

  public Event asDto(EventJson eventJson, EventType eventType) throws IOException {
    return toDtoHelper(eventType, cls -> eventJson);
  }

  public ClientEventJson toJson(ClientEvent clientEvent) throws JsonProcessingException {
    final MessagingDtoJsonMapper mapper = MessagingDtoJsonMapper.INSTANCE;
    ClientEventJson clientEventJson = mapper.toClientEventJson(clientEvent);
    EventJson eventJson;
    Event event = clientEvent.getEvent();
    switch (clientEvent.getEventType()) {
      case PING_FREQUENCY_ADJUSTMENT:
        eventJson = mapper.toPingFrequencyAdjustmentEventJson((PingFrequencyAdjustmentEvent) event);
        break;
      case SAVE_PIPELINE:
        eventJson = mapper.toPipelineSaveEventJson((PipelineSaveEvent) event);
        break;
      case SAVE_RULES_PIPELINE:
        eventJson = mapper.toPipelineSaveRulesEventJson((PipelineSaveRulesEvent) event);
        break;
      case STATUS_PIPELINE:
        eventJson = mapper.toPipelineStatusEventJson((PipelineStatusEvent) event);
        break;
      case STATUS_MULTIPLE_PIPELINES:
        eventJson = mapper.toPipelineStatusEventsJson((PipelineStatusEvents) event);
        break;
      case ACK_EVENT:
        eventJson = mapper.toAckEventJson((AckEvent) event);
        break;
      case SDC_INFO_EVENT:
        eventJson = mapper.toSDCInfoEventJson((SDCInfoEvent) event);
        break;
      case SDC_PROCESS_METRICS_EVENT:
        eventJson = mapper.toSDCMetricsEventJson((SDCProcessMetricsEvent) event);
        break;
      case SYNC_ACL:
        eventJson = mapper.toSyncAclEventJson((SyncAclEvent)event);
        break;
      case STOP_DELETE_PIPELINE:
        eventJson = mapper.toPipelineStopAndDeleteEventJson((PipelineStopAndDeleteEvent) event);
        break;
      case START_PIPELINE:
        eventJson = mapper.toPipelineStartEventJson((PipelineStartEvent) event);
        break;
      case PREVIEW_PIPELINE:
        eventJson = mapper.toPipelinePreviewEventJson((PipelinePreviewEvent) event);
        break;
      case STOP_PIPELINE:
        eventJson = mapper.toPipelineStopEventJson((PipelineStopEvent) event);
        break;
      case VALIDATE_PIPELINE:
        eventJson = mapper.toPipelineValidateEventJson((PipelineValidateEvent) event);
        break;
      case RESET_OFFSET_PIPELINE:
        eventJson = mapper.toPipelineResetEventJson((PipelineResetEvent) event);
        break;
      case DELETE_HISTORY_PIPELINE:
        eventJson = mapper.toPipelineHistoryDeleteEventJson((PipelineHistoryDeleteEvent) event);
        break;
      case DELETE_PIPELINE:
        eventJson = mapper.toPipelineDeleteEventJson((PipelineDeleteEvent) event);
        break;
      case BLOB_STORE:
        eventJson = mapper.toBlobStoreEventJson((BlobStoreEvent) event);
        break;
      case BLOB_DELETE:
        eventJson = mapper.toBlobDeleteEventJson((BlobDeleteEvent) event);
        break;
      case BLOB_DELETE_VERSION:
        eventJson = mapper.toBlobDeleteVersionEventJson((BlobDeleteVersionEvent) event);
        break;
      case SAVE_CONFIGURATION:
        eventJson = mapper.toSaveConfigurationEventJson((SaveConfigurationEvent) event);
        break;
      case SSO_DISCONNECTED_MODE_CREDENTIALS:
        eventJson = mapper.toDisconectedSsoCredentialsJson((DisconnectedSsoCredentialsEvent) event);
        break;
      default:
        throw new IllegalStateException("Unrecognized event type: " + clientEvent.getEventType());
    }
    // Map payload
    clientEventJson.setPayload(serialize(eventJson));
    return clientEventJson;
  }

  public List<ClientEventJson> toJson(List<ClientEvent> clientEventList) throws JsonProcessingException {
    List<ClientEventJson> clientEventJsonList = new ArrayList<>();
    for (ClientEvent clientEvent: clientEventList) {
      clientEventJsonList.add(toJson(clientEvent));
    }
    return clientEventJsonList;
  }

  public ServerEvent asDto(ServerEventJson serverEventJson) throws IOException {
    final MessagingDtoJsonMapper mapper = MessagingDtoJsonMapper.INSTANCE;
    ServerEvent serverEvent = mapper.asServerEventDto(serverEventJson);
    EventType eventType = serverEvent.getEventType();
    if (eventType == null) {
      return null;
    }
    final String payload = serverEventJson.getPayload();
    serverEvent.setEvent(toDtoHelper(eventType, cls -> deserialize(payload, cls)));
    return serverEvent;
  }
}
