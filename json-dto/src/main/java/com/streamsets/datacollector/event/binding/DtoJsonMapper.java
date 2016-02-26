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

import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveRulesEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.json.EventTypeJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveRulesEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;

import fr.xebia.extras.selma.Mapper;
import fr.xebia.extras.selma.Selma;

@Mapper
public abstract class DtoJsonMapper {
  static final DtoJsonMapper INSTANCE = Selma.builder(DtoJsonMapper.class).build();

  abstract EventTypeJson toEventTypeJson(EventType eventType);

  abstract EventType asEventTypeDto(EventTypeJson eventTypeJson);

  abstract PipelineSaveEventJson toPipelineSaveEventJson(PipelineSaveEvent pipelineSaveEvent);

  abstract PipelineSaveEvent asPipelineSaveEventDto(PipelineSaveEventJson pipelineSaveEventJson);

  abstract PipelineSaveRulesEventJson toPipelineSaveRulesEventJson(PipelineSaveRulesEvent pipelineSaveRulesEvent);

  abstract PipelineSaveRulesEvent asPipelineSaveRulesEventDto(PipelineSaveRulesEventJson pipelineSaveRulesEventJson);

  abstract PipelineBaseEventJson toPipelineBaseEventJson(PipelineBaseEvent event);

  abstract PipelineBaseEvent asPipelineBaseEventDto(PipelineBaseEventJson pipelineActionEventJson);

  abstract PingFrequencyAdjustmentEventJson toPingFrequencyAdjustmentEventJson(PingFrequencyAdjustmentEvent pingFrequencyEvent);

  abstract PingFrequencyAdjustmentEvent asPingFrequencyAdjustmentEventDto(PingFrequencyAdjustmentEventJson pingFrequencyEventJson);

  abstract PipelineStatusEventJson toPipelineStatusEventJson(PipelineStatusEvent pipelineStatusEvent);

  abstract PipelineStatusEvent asPipelineStatusEventDto(PipelineStatusEventJson pipelineStatusEvent);

}




