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

import java.util.UUID;

import org.junit.Test;

import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.config.json.PipelineConfigAndRulesJson;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;

public class TestDtoJsonMapper {

  @Test
  public void testPipelineBaseEventJsonToFromDto() {
    UUID uuid = UUID.randomUUID();
    PipelineBaseEvent pde = new PipelineBaseEvent(uuid, "name1", "rev1", "user1");
    PipelineBaseEventJson pdeJson =
      (PipelineBaseEventJson) JsonToFromDto.getInstance().toJson(EventType.START_PIPELINE, pde);
    assertEquals(uuid, pdeJson.getUuid());
    assertEquals("name1", pdeJson.getName());
    assertEquals("rev1", pdeJson.getRev());
    assertEquals("user1", pdeJson.getUser());

    PipelineBaseEventJson pdeJson2 = new PipelineBaseEventJson();
    pdeJson2.setUuid(uuid);
    pdeJson2.setName("name2");
    pdeJson2.setRev("rev2");
    pdeJson2.setUser("user2");
    PipelineBaseEvent pdeEvent =
      (PipelineBaseEvent) JsonToFromDto.getInstance().asDto(EventType.START_PIPELINE, pdeJson2);
    assertEquals("name2", pdeEvent.getName());
    assertEquals("rev2", pdeEvent.getRev());
    assertEquals("user2", pdeEvent.getUser());
    assertEquals(uuid, pdeEvent.getUuid());
  }

  @Test
  public void testPipelineSaveEventJsonToFromDto() {
    UUID uuid = UUID.randomUUID();
    PipelineConfigAndRules pipelineConfigAndRules = new PipelineConfigAndRules("config", "rules");
    PipelineSaveEvent pipelineSaveEvent =
      new PipelineSaveEvent(uuid, "name1", "rev1", "user1", "desc", pipelineConfigAndRules);
    PipelineSaveEventJson pdeJson =
      (PipelineSaveEventJson) JsonToFromDto.getInstance().toJson(EventType.SAVE_PIPELINE, pipelineSaveEvent);
    assertEquals(uuid, pdeJson.getUuid());
    assertEquals("name1", pdeJson.getName());
    assertEquals("rev1", pdeJson.getRev());
    assertEquals("user1", pdeJson.getUser());
    assertEquals("config", pdeJson.getPipelineConfigurationAndRules().getPipelineConfig());
    assertEquals("rules", pdeJson.getPipelineConfigurationAndRules().getPipelineRules());

    PipelineSaveEventJson pdeJson2 = new PipelineSaveEventJson();
    PipelineConfigAndRulesJson pipelineConfigAndRulesJson = new PipelineConfigAndRulesJson();
    pipelineConfigAndRulesJson.setPipelineConfig("config");
    pipelineConfigAndRulesJson.setPipelineRules("rules");
    pdeJson2.setUuid(uuid);
    pdeJson2.setName("name2");
    pdeJson2.setRev("rev2");
    pdeJson2.setUser("user2");
    pdeJson2.setPipelineConfigurationAndRules(pipelineConfigAndRulesJson);
    PipelineSaveEvent pdeEvent =
      (PipelineSaveEvent) JsonToFromDto.getInstance().asDto(EventType.SAVE_PIPELINE, pdeJson2);
    assertEquals("name2", pdeEvent.getName());
    assertEquals("rev2", pdeEvent.getRev());
    assertEquals("user2", pdeEvent.getUser());
    assertEquals(uuid, pdeEvent.getUuid());
    assertEquals("config", pdeEvent.getPipelineConfigurationAndRules().getPipelineConfig());
    assertEquals("rules", pdeEvent.getPipelineConfigurationAndRules().getPipelineRules());
  }

}
