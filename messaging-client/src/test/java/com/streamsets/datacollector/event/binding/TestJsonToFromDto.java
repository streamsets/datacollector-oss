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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.event.dto.PingFrequencyAdjustmentEvent;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.json.BlobStoreEventJson;
import com.streamsets.datacollector.event.json.DynamicPreviewEventJson;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.PingFrequencyAdjustmentEventJson;
import com.streamsets.datacollector.event.json.PipelineDeleteEventJson;
import com.streamsets.datacollector.event.json.PipelinePreviewEventJson;
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
    PipelineStartEvent pse = new PipelineStartEvent("name1", "rev1", "user1", null, Collections.emptyList());
    ClientEvent clientEvent = new ClientEvent(uuid.toString(), Arrays.asList("SDC1"),
      true, false, EventType.START_PIPELINE, pse, "org1");
    String payload = MessagingJsonToFromDto.INSTANCE.serialize(MessagingDtoJsonMapper.INSTANCE.toPipelineStartEventJson(
        pse
    ));

    List<ClientEventJson> clientJson = MessagingJsonToFromDto.INSTANCE.toJson(Arrays.asList(clientEvent));
    assertEquals(1, clientJson.size());
    assertEquals(uuid.toString(), clientJson.get(0).getEventId());
    assertEquals(payload, clientJson.get(0).getPayload());
    assertEquals(false, clientJson.get(0).isAckEvent());
    assertEquals(true, clientJson.get(0).isRequiresAck());
    assertEquals("org1", clientJson.get(0).getOrgId());
    assertEquals(EventType.START_PIPELINE, EventType.fromValue(clientJson.get(0).getEventTypeId()));
    PipelineStartEvent actualEvent = MessagingJsonToFromDto.INSTANCE.deserialize(payload, PipelineStartEvent.class);
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

    PipelineSaveEventJson pseJson = MessagingDtoJsonMapper.INSTANCE.toPipelineSaveEventJson(pipelineSaveEvent);

    List<ServerEventJson> serverJsonList = new ArrayList<>();
    ServerEventJson serverEventJson = new ServerEventJson();
    serverEventJson.setEventId(uuid.toString());
    serverEventJson.setEventTypeId(EventType.SAVE_PIPELINE.getValue());
    serverEventJson.setAckEvent(false);
    serverEventJson.setRequiresAck(true);
    serverEventJson.setFrom("JOB_RUNNER");
    serverEventJson.setOrgId("org1");
    serverEventJson.setPayload(MessagingJsonToFromDto.INSTANCE.serialize(pseJson));
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

  @Test
  public void testMissingTypeId() throws Exception {
    // this is a test case for SDC-10844
    // basically, deserialization should no longer require the "@class" property to be present
    // since polymorphic deserialization has been disabled
    ServerEventJson serverEventJson = new ServerEventJson();
    serverEventJson.setEventId("1");
    serverEventJson.setEventTypeId(EventType.PING_FREQUENCY_ADJUSTMENT.getValue());
    final long pingFreq = 100l;
    serverEventJson.setPayload("{\"pingFrequency\": " + pingFreq + "}");

    final ServerEvent serverEvent = MessagingJsonToFromDto.INSTANCE.asDto(serverEventJson);
    assertThat(serverEvent, notNullValue());
    assertThat(serverEvent.getEvent(), instanceOf(PingFrequencyAdjustmentEvent.class));
    final PingFrequencyAdjustmentEvent pingFreqEvent = (PingFrequencyAdjustmentEvent) serverEvent.getEvent();
    assertThat(pingFreqEvent, notNullValue());
    assertThat(pingFreqEvent.getPingFrequency(), equalTo(pingFreq));
  }

  @Test
  public void testDynamicPreventEventSerializeAndDeserialize() throws Exception {

    final DynamicPreviewEventJson.Builder builder = new DynamicPreviewEventJson.Builder();

    final PipelinePreviewEventJson previewEvent = new PipelinePreviewEventJson();
    previewEvent.setBatches(1);
    previewEvent.setBatchSize(2);
    previewEvent.setSkipLifecycleEvents(true);
    previewEvent.setStopStage("stopStage");
    previewEvent.setTestOrigin(false);
    previewEvent.setTimeoutMillis(100l);
    builder.setPreviewEvent(previewEvent, EventType.PREVIEW_PIPELINE.getValue());

    final PipelineSaveEventJson saveEvent = new PipelineSaveEventJson();
    saveEvent.setUser("user");
    saveEvent.setName("name");
    saveEvent.setRev("rev");
    builder.addBeforeAction(saveEvent, EventType.SAVE_PIPELINE.getValue());

    final PipelineDeleteEventJson deleteEvent = new PipelineDeleteEventJson();
    deleteEvent.setUser("user");
    deleteEvent.setName("name");
    deleteEvent.setRev("rev");
    builder.addAfterAction(deleteEvent, EventType.DELETE_PIPELINE.getValue());

    final DynamicPreviewEventJson event = builder.build();

    final ObjectMapper objectMapper = MessagingJsonToFromDto.INSTANCE.getObjectMapper();
    final String serialized = objectMapper.writeValueAsString(event);

    final DynamicPreviewEventJson deserialized = objectMapper.readValue(serialized, DynamicPreviewEventJson.class);

    assertActions(
        event.getBeforeActionsEventTypeIds(),
        deserialized.getBeforeActionsEventTypeIds(),
        event.getBeforeActions(),
        deserialized.getBeforeActions(),
        objectMapper
    );

    assertActions(
        event.getAfterActionsEventTypeIds(),
        deserialized.getAfterActionsEventTypeIds(),
        event.getAfterActions(),
        deserialized.getAfterActions(),
        objectMapper
    );

    assertActions(
        Collections.singletonList(event.getPreviewEventTypeId()),
        Collections.singletonList(deserialized.getPreviewEventTypeId()),
        Collections.singletonList(event.getPreviewEvent()),
        Collections.singletonList(deserialized.getPreviewEvent()),
        objectMapper
    );
  }

  private static void assertActions(
      List<Integer> originalTypeIds,
      List<Integer> deserializedTypeIds,
      List<EventJson> originalEvents,
      List<EventJson> deserializedEvents,
      ObjectMapper eventJsonComparisonObjectMapper
  ) throws JsonProcessingException {
    assertThat(deserializedTypeIds.size(), equalTo(originalTypeIds.size()));
    assertThat(deserializedEvents.size(), equalTo(originalEvents.size()));
    for (int i = 0; i < deserializedTypeIds.size(); i++) {
      assertThat(deserializedTypeIds.get(i), equalTo(originalTypeIds.get(i)));
      final EventJson deserializedEvent = deserializedEvents.get(i);
      final EventJson originalEvent = originalEvents.get(i);

      final String deserializedEventJsonText = eventJsonComparisonObjectMapper.writeValueAsString(deserializedEvent);
      final String originalEventJsonText = eventJsonComparisonObjectMapper.writeValueAsString(originalEvent);
      assertThat(deserializedEventJsonText, equalTo(originalEventJsonText));
    }
  }
}
