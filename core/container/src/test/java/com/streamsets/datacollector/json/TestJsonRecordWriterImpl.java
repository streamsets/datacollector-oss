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
package com.streamsets.datacollector.json;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.testing.fieldbuilder.MapFieldBuilder;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TestJsonRecordWriterImpl {

  @Test
  public void testMultipleObjects() throws Exception {
    StringWriter writer = new StringWriter();

    JsonRecordWriterImpl jsonRecordWriter = new JsonRecordWriterImpl(writer, Mode.MULTIPLE_OBJECTS);

    Record record = new RecordImpl("stage", "id", null, null);
    record.set(Field.create(10));

    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.close();

    Assert.assertEquals("10\n10\n10", writer.toString());
  }

  @Test
  public void testArray() throws Exception {
    StringWriter writer = new StringWriter();

    JsonRecordWriterImpl jsonRecordWriter = new JsonRecordWriterImpl(writer, Mode.ARRAY_OBJECTS);

    Record record = new RecordImpl("stage", "id", null, null);
    record.set(Field.create(666));

    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.write(record);
    jsonRecordWriter.close();

    Assert.assertEquals("[666,666,666]", writer.toString());
  }

  @Test
  public void testZonedDateTime() throws Exception {
    ZonedDateTime now = ZonedDateTime.now();
    StringWriter writer = new StringWriter();
    JsonRecordWriterImpl jsonRecordWriter = new JsonRecordWriterImpl(writer, Mode.MULTIPLE_OBJECTS);

    Record record = new RecordImpl("stage", "id", null, null);
    record.set(Field.createZonedDateTime(now));
    jsonRecordWriter.write(record);
    jsonRecordWriter.close();

    Assert.assertEquals("\"" + now.format(DateTimeFormatter.ISO_ZONED_DATE_TIME) + "\"", writer.toString());
  }

  @Test
  public void testComplexField() throws Exception {
    final String expectedJson = IOUtils.toString(this.getClass().getResource(
        "/com/streamsets/datacollector/json/complex_field_expected.json"),
        Charsets.UTF_8
    );

    StringWriter writer = new StringWriter();
    JsonRecordWriterImpl jsonRecordWriter = new JsonRecordWriterImpl(writer, Mode.ARRAY_OBJECTS);

    Record record = new RecordImpl("stage", "id", null, null);

    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.startMap("map1")
        .startMap("map1-1").add("map1-1-a", "a").add("map1-1-b", "b").end()
        .startMap("map1-2").add("map1-2-c", "c").add("map1-2-d", "d").end()
        .end()
        .startList("lists")
        .startList("list1").add("list1-1").add("list1-2").add("list1-3").end()
        .startList("list2").add("list2-1").add("list2-2").add("list2-3").end()
        .end()
        .startMap("map2")
        .startMap("map2-1")
        .startMap("map2-1-1").add("map2-1-1-a", "a").add("map2-1-1-b", "b").end()
        .startMap("map2-1-2").add("map2-1-2-c", "c").add("map2-1-2-d", "d").end()
        .end()
        .startMap("map2-2")
        .startMap("map2-2-1").add("map2-2-1-a-int", 1).add("map2-2-1-b-date", new Date(1500000000)).end()
        .startMap("map2-2-2").add("map2-2-2-c-bytes", new byte[] {0, 1, 2}).add("map2-2-2-d-s", (short) 222).end()
        .end()
        .end()
        .startList("list3").add("list3-1").add(3.2D).add(33L).end();

    record.set(builder.build());

    jsonRecordWriter.write(record);
    jsonRecordWriter.close();

    Assert.assertEquals(expectedJson, writer.toString());
  }
}
