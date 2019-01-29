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
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Arrays;

import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.PipelineDeleteEvent;
import com.streamsets.datacollector.event.dto.PipelineHistoryDeleteEvent;
import com.streamsets.datacollector.event.dto.PipelineResetEvent;
import com.streamsets.datacollector.event.dto.PipelineStartEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvent;
import com.streamsets.datacollector.event.dto.PipelineStatusEvents;
import com.streamsets.datacollector.event.dto.PipelineStopEvent;
import com.streamsets.datacollector.event.dto.PipelineValidateEvent;
import com.streamsets.datacollector.event.dto.WorkerInfo;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventJson;
import com.streamsets.datacollector.event.json.PipelineStatusEventsJson;
import com.streamsets.datacollector.event.json.WorkerInfoJson;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.json.AclJson;
import com.streamsets.lib.security.acl.json.ResourceTypeJson;
import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.config.dto.PipelineConfigAndRules;
import com.streamsets.datacollector.event.dto.PipelineBaseEvent;
import com.streamsets.datacollector.event.dto.PipelineSaveEvent;
import com.streamsets.datacollector.event.json.PipelineBaseEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.execution.PipelineStatus;


public class TestDtoJsonMapper {

  interface DtoJsonAssertions<D extends Event, J extends EventJson> {
    void doAssertions(D original, J json, D converted);
  }

  interface DtoToJson <D extends Event, J extends EventJson> {
    J toJson(D dto);
  }

  interface JsonToDto <D extends Event, J extends EventJson> {
    D toDto(J json);
  }

  private static <D extends PipelineBaseEvent, J extends PipelineBaseEventJson> void assertPipelineBaseEventConversions(
      D original,
      J json,
      D converted
  ) {
    assertThat(json.getName(), equalTo(original.getName()));
    assertThat(converted.getName(), equalTo(original.getName()));

    assertThat(json.getRev(), equalTo(original.getRev()));
    assertThat(converted.getRev(), equalTo(original.getRev()));

    assertThat(json.getUser(), equalTo(original.getUser()));
    assertThat(converted.getUser(), equalTo(original.getUser()));
  }

  private static <D extends Event, J extends EventJson> void assertDtoJsonConversion(
      D originalEvent,
      DtoToJson<D, J> toJson,
      JsonToDto<D, J> toDto,
      DtoJsonAssertions<D, J> assertionFunction
  ) {
    final J eventJson = toJson.toJson(originalEvent);

    final D convertedEvent = toDto.toDto(eventJson);

    assertionFunction.doAssertions(originalEvent, eventJson, convertedEvent);
  }

  @Test
  public void testPipelineStartEventConversion() {
    final PipelineStartEvent origEvent = new PipelineStartEvent();
    origEvent.setName("name");
    origEvent.setRev("rev");
    origEvent.setUser("user");

    assertDtoJsonConversion(origEvent,
        MessagingDtoJsonMapper.INSTANCE::toPipelineStartEventJson,
        MessagingDtoJsonMapper.INSTANCE::asPipelineStartEventDto,
        TestDtoJsonMapper::assertPipelineBaseEventConversions
    );
  }

  @Test
  public void testPipelineStopEventConversion() {
    final PipelineStopEvent origEvent = new PipelineStopEvent();
    origEvent.setName("name");
    origEvent.setRev("rev");
    origEvent.setUser("user");

    assertDtoJsonConversion(origEvent,
        MessagingDtoJsonMapper.INSTANCE::toPipelineStopEventJson,
        MessagingDtoJsonMapper.INSTANCE::asPipelineStopEventDto,
        TestDtoJsonMapper::assertPipelineBaseEventConversions
    );
  }

  @Test
  public void testPipelineDeleteEventConversion() {
    final PipelineDeleteEvent origEvent = new PipelineDeleteEvent();
    origEvent.setName("name");
    origEvent.setRev("rev");
    origEvent.setUser("user");

    assertDtoJsonConversion(origEvent,
        MessagingDtoJsonMapper.INSTANCE::toPipelineDeleteEventJson,
        MessagingDtoJsonMapper.INSTANCE::asPipelineDeleteEventDto,
        TestDtoJsonMapper::assertPipelineBaseEventConversions
    );
  }

  @Test
  public void testPipelineHistoryDeleteEventConversion() {
    final PipelineHistoryDeleteEvent origEvent = new PipelineHistoryDeleteEvent();
    origEvent.setName("name");
    origEvent.setRev("rev");
    origEvent.setUser("user");

    assertDtoJsonConversion(origEvent,
        MessagingDtoJsonMapper.INSTANCE::toPipelineHistoryDeleteEventJson,
        MessagingDtoJsonMapper.INSTANCE::asPipelineHistoryDeleteEventDto,
        TestDtoJsonMapper::assertPipelineBaseEventConversions
    );
  }

  @Test
  public void testPipelineResetEventConversion() {
    final PipelineResetEvent origEvent = new PipelineResetEvent();
    origEvent.setName("name");
    origEvent.setRev("rev");
    origEvent.setUser("user");

    assertDtoJsonConversion(origEvent,
        MessagingDtoJsonMapper.INSTANCE::toPipelineResetEventJson,
        MessagingDtoJsonMapper.INSTANCE::asPipelineResetEventDto,
        TestDtoJsonMapper::assertPipelineBaseEventConversions
    );
  }

  @Test
  public void testPipelineValidateEventConversion() {
    final PipelineValidateEvent origEvent = new PipelineValidateEvent();
    origEvent.setName("name");
    origEvent.setRev("rev");
    origEvent.setUser("user");

    assertDtoJsonConversion(origEvent,
        MessagingDtoJsonMapper.INSTANCE::toPipelineValidateEventJson,
        MessagingDtoJsonMapper.INSTANCE::asPipelineValidateEventDto,
        TestDtoJsonMapper::assertPipelineBaseEventConversions
    );
  }

  @Test
  public void testPipelineSaveEventJson() throws Exception {
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
    assertEquals("config", pseJson.getPipelineConfigurationAndRules().getPipelineConfig());
    assertEquals("rules", pseJson.getPipelineConfigurationAndRules().getPipelineRules());
    assertEquals("name1", pseJson.getName());
    assertEquals("rev1", pseJson.getRev());
    assertEquals("user1", pseJson.getUser());
    assertEquals("desc", pseJson.getDescription());
    AclJson aclJson = pseJson.getAcl();
    Assert.assertNotNull(aclJson);
    Assert.assertEquals("resourceId", aclJson.getResourceId());
    Assert.assertEquals("user1", aclJson.getLastModifiedBy());
    Assert.assertEquals(time, aclJson.getLastModifiedOn());
    Assert.assertNull(aclJson.getResourceOwner());
    Assert.assertEquals(ResourceTypeJson.PIPELINE, aclJson.getResourceType());
    pipelineSaveEvent = MessagingDtoJsonMapper.INSTANCE.asPipelineSaveEventDto(pseJson);
    assertEquals("name1", pipelineSaveEvent.getName());
    assertEquals("rev1", pipelineSaveEvent.getRev());
    assertEquals("user1", pipelineSaveEvent.getUser());
    assertEquals("config", pipelineSaveEvent.getPipelineConfigurationAndRules().getPipelineConfig());
    assertEquals("rules", pipelineSaveEvent.getPipelineConfigurationAndRules().getPipelineRules());
  }

  @Test
  public void testPipelineStatusEventJson() throws Exception {
    WorkerInfo workerInfo = new WorkerInfo();
    workerInfo.setWorkerURL("workerURL");
    workerInfo.setWorkerId("slaveId");
    Acl acl = new Acl();
    acl.setResourceId("resource1");
    PipelineStatusEvent pipelineStatusEvent = new PipelineStatusEvent("name",
        "title",
        "rev",
        -1,
        true,
        PipelineStatus.RUNNING,
        "message",
        Arrays.asList(workerInfo),
        null,
        null,
        true,
        "offset",
        2,
        acl,
        10
    );
    PipelineStatusEvents pipelineStatusEvents = new PipelineStatusEvents();
    pipelineStatusEvents.setPipelineStatusEventList(Arrays.asList(pipelineStatusEvent));
    PipelineStatusEventsJson pseJson = MessagingDtoJsonMapper.INSTANCE.toPipelineStatusEventsJson(pipelineStatusEvents);
    PipelineStatusEventJson pipelineStatusEventJson = pseJson.getPipelineStatusEventList().get(0);
    assertEquals("name", pipelineStatusEventJson.getName());
    assertEquals("rev", pipelineStatusEventJson.getRev());
    assertEquals("offset", pipelineStatusEventJson.getOffset());
    assertEquals(-1, pipelineStatusEventJson.getTimeStamp());
    assertEquals(acl.getResourceId(), pipelineStatusEventJson.getAcl().getResourceId());
    assertTrue(pipelineStatusEventJson.isClusterMode());
    assertEquals(10, pipelineStatusEventJson.getRunnerCount());
    WorkerInfoJson workerInfoJson = pipelineStatusEventJson.getWorkerInfos().iterator().next();
    assertEquals("workerURL", workerInfoJson.getWorkerURL());
    assertEquals("slaveId", workerInfoJson.getWorkerId());


    pipelineStatusEvents = MessagingDtoJsonMapper.INSTANCE.asPipelineStatusEventsDto(pseJson);
    pipelineStatusEvent = pipelineStatusEvents.getPipelineStatusEventList().get(0);
    assertEquals("name", pipelineStatusEvent.getName());
    assertEquals("rev", pipelineStatusEvent.getRev());
    assertEquals(-1, pipelineStatusEvent.getTimeStamp());
    workerInfo = pipelineStatusEvent.getWorkerInfos().iterator().next();
    assertEquals("workerURL", workerInfo.getWorkerURL());
    assertEquals("slaveId", workerInfo.getWorkerId());
    assertTrue(pipelineStatusEvent.isClusterMode());
  }

}
