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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.config.json.PipelineConfigAndRulesJson;
import com.streamsets.datacollector.event.dto.ClientEvent;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.dto.ServerEvent;
import com.streamsets.datacollector.event.json.ClientEventJson;
import com.streamsets.datacollector.event.json.EventTypeJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;

public class TestDtoJsonMapper {

  @Test
  public void testPipelineClientEventJson() throws Exception {
    UUID uuid = UUID.randomUUID();
    PipelineBaseEvent pde = new PipelineBaseEvent("name1", "rev1", "user1");
    ClientEvent clientEvent = new ClientEvent(uuid.toString(), Arrays.asList("SDC1"),
      true, false, EventType.START_PIPELINE, pde);
    String payload = JsonToFromDto.getInstance().serialize(pde);

    List<ClientEventJson> clientJson = JsonToFromDto.getInstance().toJson(Arrays.asList(clientEvent));
    assertEquals(1, clientJson.size());
    assertEquals(uuid.toString(), clientJson.get(0).getEventId());
    assertEquals(payload, clientJson.get(0).getPayload());
    assertEquals(false, clientJson.get(0).isAckEvent());
    assertEquals(true, clientJson.get(0).isRequiresAck());
    assertEquals(EventTypeJson.START_PIPELINE, clientJson.get(0).getEventType());


    PipelineBaseEvent actualEvent = JsonToFromDto.getInstance().deserialize(payload, new TypeReference<PipelineBaseEvent>() {});
    assertEquals("name1", actualEvent.getName());
    assertEquals("rev1", actualEvent.getRev());
    assertEquals("user1", actualEvent.getUser());
  }

  @Test
  public void testPipelineServerEventJson() throws Exception {
    UUID uuid = UUID.randomUUID();
    PipelineConfigAndRules pipelineConfigAndRules = new PipelineConfigAndRules("config", "rules");
    PipelineSaveEvent pipelineSaveEvent =
     new PipelineSaveEvent("name1", "rev1", "user1", "desc", pipelineConfigAndRules);

    PipelineSaveEventJson pseJson = new PipelineSaveEventJson();
    PipelineConfigAndRulesJson pipelineConfigAndRulesJson = new PipelineConfigAndRulesJson();
    pipelineConfigAndRulesJson.setPipelineConfig("config");
    pipelineConfigAndRulesJson.setPipelineRules("rules");
    pseJson.setName("name1");
    pseJson.setRev("rev1");
    pseJson.setUser("user1");
    pseJson.setPipelineConfigurationAndRules(pipelineConfigAndRulesJson);

    List<ServerEventJson> serverJsonList = new ArrayList<ServerEventJson>();
    ServerEventJson serverEventJson = new ServerEventJson();
    serverEventJson.setEventId(uuid.toString());
    serverEventJson.setEventType(EventTypeJson.SAVE_PIPELINE);
    serverEventJson.setAckEvent(false);
    serverEventJson.setRequiresAck(true);
    serverEventJson.setFrom("JOB_RUNNER");
    serverEventJson.setPayload(JsonToFromDto.getInstance().serialize(pipelineSaveEvent));
    serverJsonList.add(serverEventJson);
    ServerEvent serverEvent = JsonToFromDto.getInstance().asDto(serverEventJson);
    assertEquals(uuid.toString(), serverEvent.getEventId());
    assertEquals(EventType.SAVE_PIPELINE, serverEvent.getEventType());
    assertEquals("JOB_RUNNER", serverEvent.getFrom());
    assertEquals(false, serverEvent.isAckEvent());
    assertEquals(true, serverEvent.isRequiresAck());
    assertTrue(serverEvent.getEvent() instanceof PipelineSaveEvent);
    PipelineSaveEvent saveEvent = (PipelineSaveEvent)serverEvent.getEvent();
    assertEquals("name1", saveEvent.getName());
    assertEquals("rev1", saveEvent.getRev());
    assertEquals("user1", saveEvent.getUser());
  }
}
