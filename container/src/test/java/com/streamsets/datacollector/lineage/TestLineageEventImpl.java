/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.lineage;

import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.junit.Assert;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.List;

public class TestLineageEventImpl {

  @Test
  public void testSimpleConfiguration() {
    LineageEventImpl lineageEvent = new LineageEventImpl(
        LineageEventType.START,
        "test",
        "admin",
        System.currentTimeMillis(),
        "446355a9-e20b-4460-a3f1-14fdf5d39b81",
        "0a7ab90b-f1a7-435e-ad11-141f39a1a791",
        "http://localhost:18630",
        "testStage",
        "this is a test pipeline",
        "v1",
        null, // shouldn't cause NPE
        null // shouldn't cause NPE
    );
    // Start event should have description in SpecificAttribute
    Assert.assertEquals("this is a test pipeline", lineageEvent.getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION));

    // Pipeline ID should be stored in general attribute
    Assert.assertEquals("446355a9-e20b-4460-a3f1-14fdf5d39b81", lineageEvent.getPipelineId());
    // Pipeline version should be stored in properties
    Map<String, String> properties = lineageEvent.getProperties();
    Assert.assertEquals("v1", properties.get(LineageGeneralAttribute.PIPELINE_VERSION.getLabel()));
    Assert.assertEquals(1, lineageEvent.getProperties().size());
  }

  @Test
  public void testPasswordMasking() {
    LineageEventImpl lineageEvent = new LineageEventImpl(
      LineageEventType.START,
      "test",
      "admin",
      System.currentTimeMillis(),
      "1",
      "abcdf",
      "http://localhost:18630",
      "testStage",
      "this is a test pipeline",
      "v1",
        ImmutableMap.of("labels", ImmutableList.of("sdc", "test")),
        ImmutableMap.of(
            "param1", "vakue1",
            "password", "ChangeMe!")
    );
    Map<String, String> properties = lineageEvent.getProperties();
    Assert.assertEquals("*********", properties.get("password"));

    // try a bit more complicated key for password field
    LineageEventImpl lineageEvent2 = new LineageEventImpl(
        LineageEventType.START,
        "test",
        "admin",
        System.currentTimeMillis(),
        "1",
        "abcdf",
        "http://localhost:18630",
        "testStage",
        "this is a test pipeline",
        "v1",
        ImmutableMap.of("labels", ImmutableList.of("sdc", "test")),
        ImmutableMap.of(
            "param1", "vakue1",
            "navigator-password-1", "ChangeMe!")  // this should be picked up as password
    );
    Map<String, String> properties2 = lineageEvent2.getProperties();
    Assert.assertEquals("*********", properties2.get("navigator-password-1"));
  }

  @Test
  public void testDPMProperties() {
    LineageEventImpl lineageEvent = new LineageEventImpl(
        LineageEventType.START,
        "test",
        "admin",
        System.currentTimeMillis(),
        "1",
        "abcdf",
        "http://localhost:18630",
        "testStage",
        "this is a test pipeline",
        "v1",
        ImmutableMap.of(
            "dpm.pipeline.id", "12345",
            "dpm.pipeline.version", "3",
            "labels", ImmutableList.of("sch", "cloud")
        ),
        ImmutableMap.of(
            "param1", "value1",
            "path", "/var/run/streamsets"
        )
    );
    Assert.assertEquals("12345", lineageEvent.getPipelineId());
    Map<String, String> property = lineageEvent.getProperties();
    Assert.assertEquals("3", property.get(LineageGeneralAttribute.PIPELINE_VERSION.getLabel()));
    // below properties shouldn't be masked
    Assert.assertEquals("value1", property.get("param1"));
    Assert.assertEquals("/var/run/streamsets", property.get("path"));

    List<String> tag = lineageEvent.getTags();
    Assert.assertEquals("sch", tag.get(0));
    Assert.assertEquals("cloud", tag.get(1));
  }

}
