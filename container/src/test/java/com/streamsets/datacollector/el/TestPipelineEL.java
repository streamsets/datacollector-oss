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
package com.streamsets.datacollector.el;

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.runner.UserContext;
import com.streamsets.datacollector.store.PipelineInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class TestPipelineEL {

  @Test
  public void testUndefinedPipelineNameAndVersion() {
    PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(
        5,
        5,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        "",
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList(),
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    PipelineEL.setConstantsInContext(
        pipelineConfiguration,
        new UserContext(null, false, false),
        System.currentTimeMillis()
    );
    Assert.assertEquals("UNDEFINED", PipelineEL.name());
    Assert.assertEquals("UNDEFINED", PipelineEL.version());
    Assert.assertEquals("UNDEFINED", PipelineEL.id());
    Assert.assertEquals("UNDEFINED", PipelineEL.title());
    Assert.assertEquals("UNDEFINED", PipelineEL.user());
  }

  @Test
  public void testPipelineNameAndVersion() {
    Map<String, Object> metadata = ImmutableMap.<String, Object>of(PipelineEL.PIPELINE_VERSION_VAR, "3");
    UUID uuid = UUID.randomUUID();
    PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(
        5,
        5,
        "pipelineId",
        uuid,
        "label",
        "",
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList(),
        null,
        null,
        Collections.emptyList(),
        Collections.emptyList()
    );
    pipelineConfiguration.setMetadata(metadata);
    pipelineConfiguration.setPipelineInfo(new PipelineInfo("hello" , "label", "", new Date(), new Date(), "", "", "", uuid, false, metadata, null, null));
    Date startTime = new Date();
    PipelineEL.setConstantsInContext(
        pipelineConfiguration,
        new UserContext("test-user", false, false),
        startTime.getTime()
    );
    Assert.assertEquals("hello", PipelineEL.name());
    Assert.assertEquals("3", PipelineEL.version());
    Assert.assertEquals("hello", PipelineEL.id());
    Assert.assertEquals("label", PipelineEL.title());
    Assert.assertEquals("test-user", PipelineEL.user());
    Assert.assertEquals(startTime, PipelineEL.startTime());
  }
}
