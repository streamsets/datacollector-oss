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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import org.junit.Assert;
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
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.ServerEventJson;

public class TestJsonToFromDto {

  @Test
  public void testPipelineClientEventJson() throws Exception {
    UUID uuid = UUID.randomUUID();
    PipelineBaseEvent pde = new PipelineBaseEvent("name1", "rev1", "user1");
    ClientEvent clientEvent = new ClientEvent(uuid.toString(), Arrays.asList("SDC1"),
      true, false, EventType.START_PIPELINE, pde, "org1");
    String payload = MessagingJsonToFromDto.INSTANCE.serialize(pde);

    List<ClientEventJson> clientJson = MessagingJsonToFromDto.INSTANCE.toJson(Arrays.asList(clientEvent));
    assertEquals(1, clientJson.size());
    assertEquals(uuid.toString(), clientJson.get(0).getEventId());
    assertEquals(payload, clientJson.get(0).getPayload());
    assertEquals(false, clientJson.get(0).isAckEvent());
    assertEquals(true, clientJson.get(0).isRequiresAck());
    assertEquals("org1", clientJson.get(0).getOrgId());
    assertEquals(EventType.START_PIPELINE, EventType.fromValue(clientJson.get(0).getEventTypeId()));
    PipelineBaseEvent actualEvent = MessagingJsonToFromDto.INSTANCE.deserialize(payload, new TypeReference<PipelineBaseEvent>() {});
    assertEquals("name1", actualEvent.getName());
    assertEquals("rev1", actualEvent.getRev());
    assertEquals("user1", actualEvent.getUser());
  }

  @Test
  public void testPipelineServerEventJson() throws Exception {
    UUID uuid = UUID.randomUUID();
    PipelineConfigAndRules pipelineConfigAndRules = new PipelineConfigAndRules("config", "rules");

    long time = System.currentTimeMillis();
    Acl acl = new Acl();
    acl.setResourceId("resourceId");
    acl.setLastModifiedBy("user1");
    acl.setLastModifiedOn(time);
    acl.setResourceType(ResourceType.PIPELINE);
    Permission permission = new Permission();
    permission.setSubjectId("user1");
    acl.setPermissions(Arrays.asList(permission));

    PipelineSaveEvent pipelineSaveEvent = new PipelineSaveEvent();
    pipelineSaveEvent.setName("name1");
    pipelineSaveEvent.setRev("rev1");
    pipelineSaveEvent.setUser("user1");
    pipelineSaveEvent.setDescription("desc");
    pipelineSaveEvent.setPipelineConfigurationAndRules(pipelineConfigAndRules);
    pipelineSaveEvent.setAcl(acl);

    PipelineSaveEventJson pseJson = new PipelineSaveEventJson();
    PipelineConfigAndRulesJson pipelineConfigAndRulesJson = new PipelineConfigAndRulesJson();
    pipelineConfigAndRulesJson.setPipelineConfig("config");
    pipelineConfigAndRulesJson.setPipelineRules("rules");
    pseJson.setName("name1");
    pseJson.setRev("rev1");
    pseJson.setUser("user1");
    pseJson.setPipelineConfigurationAndRules(pipelineConfigAndRulesJson);

    List<ServerEventJson> serverJsonList = new ArrayList<>();
    ServerEventJson serverEventJson = new ServerEventJson();
    serverEventJson.setEventId(uuid.toString());
    serverEventJson.setEventTypeId(EventType.SAVE_PIPELINE.getValue());
    serverEventJson.setAckEvent(false);
    serverEventJson.setRequiresAck(true);
    serverEventJson.setFrom("JOB_RUNNER");
    serverEventJson.setOrgId("org1");
    serverEventJson.setPayload(MessagingJsonToFromDto.INSTANCE.serialize(pipelineSaveEvent));
    serverJsonList.add(serverEventJson);
    ServerEvent serverEvent = MessagingJsonToFromDto.INSTANCE.asDto(serverEventJson);
    assertEquals(uuid.toString(), serverEvent.getEventId());
    assertEquals(EventType.SAVE_PIPELINE, serverEvent.getEventType());
    assertEquals("org1", serverEvent.getOrgId());
    assertEquals("JOB_RUNNER", serverEvent.getFrom());
    assertEquals(false, serverEvent.isAckEvent());
    assertEquals(true, serverEvent.isRequiresAck());
    assertTrue(serverEvent.getEvent() instanceof PipelineSaveEvent);
    PipelineSaveEvent saveEvent = (PipelineSaveEvent)serverEvent.getEvent();
    assertEquals("name1", saveEvent.getName());
    assertEquals("rev1", saveEvent.getRev());
    assertEquals("user1", saveEvent.getUser());
  }

  @Test
  public void testUnknownEventType() throws Exception {
    ServerEventJson serverEventJson = new ServerEventJson();
    serverEventJson.setEventId("1");
    serverEventJson.setEventTypeId(100000);
    Assert.assertNull(MessagingJsonToFromDto.INSTANCE.asDto(serverEventJson));
  }
}
