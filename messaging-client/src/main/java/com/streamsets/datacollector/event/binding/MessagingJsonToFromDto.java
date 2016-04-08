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

package com.streamsets.datacollector.event.binding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.datacollector.event.binding.MessagingDtoJsonMapper;
import com.streamsets.datacollector.event.dto.AckEvent;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.SDCInfoEvent;
import com.streamsets.datacollector.event.dto.ServerEvent;
import com.streamsets.datacollector.event.json.AckEventJson;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.SDCInfoEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;


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

  public <T> T deserialize(String body, TypeReference<T> type) throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(body, type);
  }

  public String serialize(Object object) throws JsonProcessingException {
    return mapper.writeValueAsString(object);
  }

  private ClientEventJson toJson(ClientEvent clientEvent) throws JsonProcessingException {
    ClientEventJson clientEventJson = MessagingDtoJsonMapper.INSTANCE.toClientEventJson(clientEvent);
    EventJson eventJson;
    Event event = clientEvent.getEvent();
    switch (clientEvent.getEventType()) {
      case PING_FREQUENCY_ADJUSTMENT:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toPingFrequencyAdjustmentEventJson((PingFrequencyAdjustmentEvent) event);
        break;
      case SAVE_PIPELINE:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toPipelineSaveEventJson((PipelineSaveEvent) event);
        break;
      case SAVE_RULES_PIPELINE:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toPipelineSaveRulesEventJson((PipelineSaveRulesEvent) event);
        break;
      case STATUS_PIPELINE:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toPipelineStatusEventJson((PipelineStatusEvent) event);
        break;
      case ACK_EVENT:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toAckEventJson((AckEvent) event);
        break;
      case SDC_INFO_EVENT:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toSDCInfoEventJson((SDCInfoEvent) event);
        break;
      case START_PIPELINE:
      case STOP_PIPELINE:
      case VALIDATE_PIPELINE:
      case RESET_OFFSET_PIPELINE:
      case DELETE_HISTORY_PIPELINE:
      case DELETE_PIPELINE:
      case STOP_DELETE_PIPELINE:
        eventJson = MessagingDtoJsonMapper.INSTANCE.toPipelineBaseEventJson((PipelineBaseEvent) event);
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

  public ServerEvent asDto(ServerEventJson serverEventJson) throws JsonParseException, JsonMappingException,
    IOException {
    ServerEvent serverEvent = MessagingDtoJsonMapper.INSTANCE.asServerEventDto(serverEventJson);
    switch (serverEvent.getEventType()) {
      case ACK_EVENT: {
        TypeReference<AckEventJson> typeRef = new TypeReference<AckEventJson>() {
        };
        AckEventJson ackEventJson = deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent.setEvent(MessagingDtoJsonMapper.INSTANCE.asAckEventDto(ackEventJson));
        break;
      }
      case PING_FREQUENCY_ADJUSTMENT: {
        TypeReference<PingFrequencyAdjustmentEventJson> typeRef =
          new TypeReference<PingFrequencyAdjustmentEventJson>() {
          };
        PingFrequencyAdjustmentEventJson pingFrequencyAdjustmentEventJson =
          deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent
          .setEvent(MessagingDtoJsonMapper.INSTANCE.asPingFrequencyAdjustmentEventDto(pingFrequencyAdjustmentEventJson));
        break;
      }
      case SAVE_PIPELINE: {
        TypeReference<PipelineSaveEventJson> typeRef = new TypeReference<PipelineSaveEventJson>() {
        };
        PipelineSaveEventJson pipelineSaveEventJson = deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent.setEvent(MessagingDtoJsonMapper.INSTANCE.asPipelineSaveEventDto(pipelineSaveEventJson));
      }
        break;
      case SAVE_RULES_PIPELINE: {
        TypeReference<PipelineSaveRulesEventJson> typeRef = new TypeReference<PipelineSaveRulesEventJson>() {
        };
        PipelineSaveRulesEventJson pipelineSaveRulesEventJson = deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent.setEvent(MessagingDtoJsonMapper.INSTANCE.asPipelineSaveRulesEventDto(pipelineSaveRulesEventJson));
        break;
      }
      case SDC_INFO_EVENT: {
        TypeReference<SDCInfoEventJson> typeRef = new TypeReference<SDCInfoEventJson>() {
        };
        SDCInfoEventJson sdcInfoEventJson = deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent.setEvent(MessagingDtoJsonMapper.INSTANCE.asSDCInfoEventDto(sdcInfoEventJson));
        break;
      }
      case STATUS_PIPELINE: {
        TypeReference<PipelineStatusEventJson> typeRef = new TypeReference<PipelineStatusEventJson>() {
        };
        PipelineStatusEventJson pipelineStatusEventJson = deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent.setEvent(MessagingDtoJsonMapper.INSTANCE.asPipelineStatusEventDto(pipelineStatusEventJson));
        break;
      }
      case DELETE_HISTORY_PIPELINE:
      case DELETE_PIPELINE:
      case START_PIPELINE:
      case STOP_PIPELINE:
      case VALIDATE_PIPELINE:
      case RESET_OFFSET_PIPELINE:
      case STOP_DELETE_PIPELINE: {
        TypeReference<PipelineBaseEventJson> typeRef = new TypeReference<PipelineBaseEventJson>() {
        };
        PipelineBaseEventJson pipelineBaseEventJson = deserialize(serverEventJson.getPayload(), typeRef);
        serverEvent.setEvent(MessagingDtoJsonMapper.INSTANCE.asPipelineBaseEventDto(pipelineBaseEventJson));
        break;
      }
      default:
        break;
    }
    return serverEvent;
  }
}
