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
package com.streamsets.pipeline.stage.destination.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCollectdRecordConverter {
  public static final String HOSTNAME = "hostname.streamsets.net";
  public static final String PLUGIN = "interface";
  public static final String PLUGIN_INSTANCE = "lo0";
  public static final String TYPE = "if_octets";
  public static final long OCTETS_RX = 2244428004L;
  public static final long TIME_MILLIS = 1455645265108L;
  public static final long TIME_HIRES = 1562640634329947275L;

  private Record record;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    record = RecordCreator.create();

    Map<String, Field> recordValue = new HashMap<>();
    recordValue.put("host", Field.create(HOSTNAME));
    recordValue.put("plugin", Field.create(PLUGIN));
    recordValue.put("plugin_instance", Field.create(PLUGIN_INSTANCE));
    recordValue.put("type", Field.create(TYPE));
    recordValue.put("rx", Field.create(OCTETS_RX));

    record.set(Field.create(recordValue));
  }

  @Test
  public void testGetTags() throws OnRecordErrorException {
    Map<String, String> expectedTags = new ImmutableMap.Builder<String, String>()
        .put("host", HOSTNAME)
        .put("instance", PLUGIN_INSTANCE)
        .put("type", TYPE)
        .build();
    Map<String, String> tags = CollectdRecordConverter.getTags(Collections.<String>emptyList(), record);
    assertEquals(expectedTags, tags);
  }

  @Test
  public void testGetTagsWithExtraTagFields() throws OnRecordErrorException {
    GenericRecordConverterConfigBean conf = new GenericRecordConverterConfigBean();
    conf.tagFields.add("/extraTag");

    Map<String, String> expectedTags = new ImmutableMap.Builder<String, String>()
        .put("host", HOSTNAME)
        .put("instance", PLUGIN_INSTANCE)
        .put("type", TYPE)
        .put("extraTag", "tagValue")
        .build();

    record.set("/extraTag", Field.create("tagValue"));
    Map<String, String> tags = CollectdRecordConverter.getTags(conf.tagFields, record);
    assertEquals(expectedTags, tags);
  }

  @Test
  public void testGetMeasurementBaseName() throws OnRecordErrorException {
    final String expected = PLUGIN;
    String actual = CollectdRecordConverter.getMeasurementBaseName(record);

    assertEquals(expected, actual);
  }

  @Test
  public void testInvalidGetMeasurementBaseName() throws OnRecordErrorException {
    thrown.expect(OnRecordErrorException.class);
    record.delete("/plugin");
    CollectdRecordConverter.getMeasurementBaseName(record);
  }

  @Test
  public void testGetTimeMillis() throws OnRecordErrorException {
    final long expected = TIME_MILLIS;
    record.set("/time", Field.create(expected));
    long actual = CollectdRecordConverter.getTime(record);

    assertEquals(expected, actual);
  }

  @Test
  public void testGetTimeNanos() throws OnRecordErrorException {
    final long hiresTime = TIME_HIRES;
    final long expected = 1455322498879599555L;
    record.set("/time_hires", Field.create(hiresTime));
    long actual = CollectdRecordConverter.getTime(record);

    assertEquals(expected, actual);
  }

  @Test
  public void testInvalidGetTime() throws OnRecordErrorException {
    thrown.expect(OnRecordErrorException.class);
    CollectdRecordConverter.getTime(record);
  }

  @Test
  public void testGetTimePrecisionMillis() throws OnRecordErrorException {
    record.set("/time", Field.create(0L));
    assertEquals(TimeUnit.MILLISECONDS, CollectdRecordConverter.getTimePrecision(record));
  }

  @Test
  public void testGetTimePrecisionNanos() throws OnRecordErrorException {
    record.set("/time_hires", Field.create(0L));
    assertEquals(TimeUnit.NANOSECONDS, CollectdRecordConverter.getTimePrecision(record));
  }

  @Test
  public void testInvalidGetTimePrecision() throws OnRecordErrorException {
    thrown.expect(OnRecordErrorException.class);
    CollectdRecordConverter.getTimePrecision(record);
  }

  @Test
  public void testGetValueFields() throws OnRecordErrorException {
    List<String> expected = ImmutableList.of("rx");
    CollectdRecordConverter converter = new CollectdRecordConverter();
    List<String> actual = converter.getValueFields(record);
    assertEquals(expected, actual);
  }

  @Test
  public void testNoValueFields() throws OnRecordErrorException {
    thrown.expect(OnRecordErrorException.class);
    record.delete("/rx");
    CollectdRecordConverter converter = new CollectdRecordConverter();
    converter.getValueFields(record);
  }

  @Test
  public void testStripPathPrefix() {
    final String expected = "field1";
    final String actual = CollectdRecordConverter.stripPathPrefix("/some/field/path/" + expected);

    assertEquals(expected, actual);
  }

  @Test
  public void testIsValueField() {
    CollectdRecordConverter converter = new CollectdRecordConverter();
    assertTrue(converter.isValueField("/rx"));
    assertFalse(converter.isValueField("/plugin"));
  }

  @Test
  public void testGetPoints() throws OnRecordErrorException {
    RecordConverter converter = new CollectdRecordConverter();
    record.set("/time", Field.create(TIME_MILLIS));
    List<Point> points = converter.getPoints(record);

    assertEquals(1, points.size());

    final String expectedPointAsString =
        "Point [name=interface_rx, time=1455645265108, tags={host=hostname.streamsets.net, instance=lo0, " +
            "type=if_octets}, precision=MILLISECONDS, fields={value=2.244428004E9}]";
    Point point = points.get(0);
    assertEquals(expectedPointAsString, point.toString());
  }
}
