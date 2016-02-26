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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.EventTypeJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;

public class JsonToFromDto {

  private static JsonToFromDto jsonToFromDtoInstance;
  private static ObjectMapper mapper;

  private JsonToFromDto() {
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
  }

  public static JsonToFromDto getInstance() {
    if(jsonToFromDtoInstance == null) {
      jsonToFromDtoInstance = new JsonToFromDto();
    }
    return jsonToFromDtoInstance;
  }

  public <T> T deserialize(String body, TypeReference<T> type) throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(body, type);
  }

  public String serialize(Event event) throws JsonProcessingException {
    return mapper.writeValueAsString(event);
  }

  public EventTypeJson toJson(EventType eventType) {
    return DtoJsonMapper.INSTANCE.toEventTypeJson(eventType);
  }

  public EventType asDto(EventTypeJson eventTypeJson) {
    return DtoJsonMapper.INSTANCE.asEventTypeDto(eventTypeJson);
  }

  public EventJson toJson(EventType eventType, Event event) {
    EventJson eventJson = null;
    switch (eventType) {
      case PING_FREQUENCY_ADJUSTMENT:
        eventJson = DtoJsonMapper.INSTANCE.toPingFrequencyAdjustmentEventJson((PingFrequencyAdjustmentEvent) event);
        break;
      case SAVE_PIPELINE:
        eventJson = DtoJsonMapper.INSTANCE.toPipelineSaveEventJson((PipelineSaveEvent) event);
        break;
      case SAVE_RULES_PIPELINE:
        eventJson = DtoJsonMapper.INSTANCE.toPipelineSaveRulesEventJson((PipelineSaveRulesEvent) event);
        break;
      case STATUS_PIPELINE:
        eventJson = DtoJsonMapper.INSTANCE.toPipelineStatusEventJson((PipelineStatusEvent) event);
        break;
      case START_PIPELINE:
      case STOP_PIPELINE:
      case VALIDATE_PIPELINE:
      case RESET_OFFSET_PIPELINE:
      case DELETE_HISTORY_PIPELINE:
      case DELETE_PIPELINE:
        eventJson = DtoJsonMapper.INSTANCE.toPipelineBaseEventJson((PipelineBaseEvent) event);
        break;
      default:
        throw new IllegalStateException("Unrecognized event type: " + eventType);
    }
    return eventJson;
  }

  public Event asDto(EventType eventType, EventJson eventJson) {
    Event event = null;
    switch (eventType) {
      case PING_FREQUENCY_ADJUSTMENT:
        event = DtoJsonMapper.INSTANCE.asPingFrequencyAdjustmentEventDto((PingFrequencyAdjustmentEventJson) eventJson);
        break;
      case SAVE_PIPELINE:
        event = DtoJsonMapper.INSTANCE.asPipelineSaveEventDto((PipelineSaveEventJson) eventJson);
        break;
      case SAVE_RULES_PIPELINE:
        event = DtoJsonMapper.INSTANCE.asPipelineSaveRulesEventDto((PipelineSaveRulesEventJson) eventJson);
        break;
      case STATUS_PIPELINE:
        event = DtoJsonMapper.INSTANCE.asPipelineStatusEventDto((PipelineStatusEventJson) eventJson);
        break;
      case START_PIPELINE:
      case STOP_PIPELINE:
      case VALIDATE_PIPELINE:
      case RESET_OFFSET_PIPELINE:
      case DELETE_HISTORY_PIPELINE:
      case DELETE_PIPELINE:
        event = DtoJsonMapper.INSTANCE.asPipelineBaseEventDto((PipelineBaseEventJson) eventJson);
        break;
      default:
        throw new IllegalStateException("Unrecognized event type: " + eventType);
    }
    return event;
  }


}
