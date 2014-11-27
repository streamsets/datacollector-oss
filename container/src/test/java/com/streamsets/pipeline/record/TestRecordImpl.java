/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.record;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestRecordImpl {

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid1() {
    new RecordImpl((String)null, (String) null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid2() {
    new RecordImpl("s", (String) null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid3() {
    new RecordImpl(null, "s", null, null);
  }

  @Test
  public void testHeaderMethods() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    Record.Header header = record.getHeader();
    Assert.assertNull(header.getRaw());
    Assert.assertNull(header.getRawMimeType());

    record = new RecordImpl("stage", "source", new byte[0], "M");
    header = record.getHeader();
    Assert.assertArrayEquals(new byte[0], header.getRaw());
    Assert.assertEquals("M", header.getRawMimeType());

    Assert.assertEquals("stage", header.getStageCreator());
    Assert.assertEquals("source", header.getSourceId());
    Assert.assertEquals("stage", header.getStagesPath());

    Assert.assertTrue(header.getAttributeNames().isEmpty());
    try {
      header.setAttribute("a", null);
      Assert.fail();
    } catch (NullPointerException ex) {
      //expected
    }
    Assert.assertTrue(header.getAttributeNames().isEmpty());
    HeaderImpl headerImpl = ((HeaderImpl)header);
    Assert.assertTrue(headerImpl.getValues().isEmpty());
    header.setAttribute("a", "A");
    Assert.assertEquals(ImmutableSet.of("a"), header.getAttributeNames());
    Assert.assertEquals("A", header.getAttribute("a"));
    Assert.assertEquals(1, headerImpl.getValues().size());
    Assert.assertEquals("A", headerImpl.getValues().get("a"));
    header.deleteAttribute("a");
    Assert.assertNull(header.getAttribute("a"));
    Assert.assertTrue(header.getAttributeNames().isEmpty());
    Assert.assertTrue(headerImpl.getValues().isEmpty());

    record.toString();
  }

  @Test
  public void testRaw() {
    RecordImpl record = new RecordImpl("stage", "source", new byte[0], "M");
    Assert.assertArrayEquals(new byte[0], record.getHeader().getRaw());
    Assert.assertNotSame(new byte[0], record.getHeader().getRaw());
    Assert.assertNotSame(record.getHeader().getRaw(), record.getHeader().getRaw());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRawInvalid1() {
    new RecordImpl("stage", "source", new byte[0], null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRawInvalid2() {
    new RecordImpl("stage", "source", null, "M");
  }

  public static void assertIsSnapshot(Record original, Record snapshot) {
    Assert.assertEquals(original.getFieldPaths(), snapshot.getFieldPaths());
    Assert.assertEquals(original.get(), snapshot.get());
    Assert.assertEquals(original.getHeader().getAttributeNames(), snapshot.getHeader().getAttributeNames());
    for (String name : original.getHeader().getAttributeNames()) {
      Assert.assertEquals(snapshot.getHeader().getAttribute(name), original.getHeader().getAttribute(name));
    }
    for (String name : snapshot.getHeader().getAttributeNames()) {
      Assert.assertEquals(snapshot.getHeader().getAttribute(name), original.getHeader().getAttribute(name));
    }
    String randomKey = UUID.randomUUID().toString();
    try {
      snapshot.getHeader().setAttribute(randomKey, "X");
      Assert.assertEquals("X", original.getHeader().getAttribute(randomKey));
      Assert.assertEquals(snapshot.getHeader().getStageCreator(), original.getHeader().getStageCreator());
      Assert.assertEquals(snapshot.getHeader().getRaw(), original.getHeader().getRaw());
      Assert.assertEquals(snapshot.getHeader().getRawMimeType(), original.getHeader().getRawMimeType());
      Assert.assertEquals(snapshot.getHeader().getSourceId(), original.getHeader().getSourceId());
    } finally {
      snapshot.getHeader().deleteAttribute(randomKey);
    }
  }

  public static void assertIsCopy(Record original, Record copy, boolean compareStagesPath) {
    Assert.assertEquals(original.getFieldPaths(), copy.getFieldPaths());
    Assert.assertEquals(original.get(), copy.get());
    Assert.assertEquals(original.getHeader().getAttributeNames(), copy.getHeader().getAttributeNames());
    for (String name : original.getHeader().getAttributeNames()) {
      Assert.assertEquals(copy.getHeader().getAttribute(name), original.getHeader().getAttribute(name));
    }
    for (String name : copy.getHeader().getAttributeNames()) {
      Assert.assertEquals(copy.getHeader().getAttribute(name), original.getHeader().getAttribute(name));
    }
    String randomKey = UUID.randomUUID().toString();
    try {
      copy.getHeader().setAttribute(randomKey, "X");
      Assert.assertNull(original.getHeader().getAttribute(randomKey));
      if (compareStagesPath) {
        Assert.assertEquals(copy.getHeader().getStagesPath(), original.getHeader().getStagesPath());
      }
    } finally {
      copy.getHeader().deleteAttribute(randomKey);
    }
  }

  @Test
  public void testAssertIsSnapshot() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.set(Field.create(true));
    record.getHeader().setAttribute("a", "A");
    RecordImpl snapshot = record.createSnapshot();
    assertIsSnapshot(record, snapshot);
  }

  @Test
  public void testAssertIsCopy() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.set(Field.create(true));
    record.getHeader().setAttribute("a", "A");
    RecordImpl copy = record.createCopy();
    assertIsCopy(record, copy, true);
  }

  @Test
  public void testAssertIsCopyDontCompareStages() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.set(Field.create(true));
    record.getHeader().setAttribute("a", "A");
    RecordImpl copy = record.createCopy();
    copy.setStage("foo");
    assertIsCopy(record, copy, false);
  }

  @Test(expected = ComparisonFailure.class)
  public void testAssertIsCopyDiffStagesCompareStages() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    RecordImpl copy = record.createCopy();
    copy.setStage("foo");
    assertIsCopy(record, copy, true);
  }

  @Test
  public void testSnapshot() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.getHeader().setAttribute("a", "A");
    record.getHeader().setAttribute("b", "B");
    Assert.assertEquals("A", record.getHeader().getAttribute("a"));
    Assert.assertEquals("B", record.getHeader().getAttribute("b"));
    RecordImpl snapshot = record.createSnapshot();
    Assert.assertEquals("A", snapshot.getHeader().getAttribute("a"));
    Assert.assertEquals("B", snapshot.getHeader().getAttribute("b"));
    record.getHeader().setAttribute("a", "AA");
    Assert.assertEquals("AA", record.getHeader().getAttribute("a"));
    Assert.assertEquals("B", record.getHeader().getAttribute("b"));
    Assert.assertEquals("A", snapshot.getHeader().getAttribute("a"));
    Assert.assertEquals("B", snapshot.getHeader().getAttribute("b"));
    snapshot.getHeader().setAttribute("a", "AAA");
    Assert.assertEquals("AA", record.getHeader().getAttribute("a"));
    Assert.assertEquals("AAA", snapshot.getHeader().getAttribute("a"));
    snapshot.getHeader().setAttribute("b", "BB");
    Assert.assertEquals("BB", record.getHeader().getAttribute("b"));
    Assert.assertEquals("BB", snapshot.getHeader().getAttribute("b"));
  }

  @Test
  public void testCopy() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.getHeader().setAttribute("a", "A");
    record.getHeader().setAttribute("b", "B");
    Assert.assertEquals("A", record.getHeader().getAttribute("a"));
    Assert.assertEquals("B", record.getHeader().getAttribute("b"));
    RecordImpl copy = record.createCopy();
    Assert.assertEquals("A", copy.getHeader().getAttribute("a"));
    Assert.assertEquals("B", copy.getHeader().getAttribute("b"));
    record.getHeader().setAttribute("a", "AA");
    Assert.assertEquals("AA", record.getHeader().getAttribute("a"));
    Assert.assertEquals("B", record.getHeader().getAttribute("b"));
    Assert.assertEquals("A", copy.getHeader().getAttribute("a"));
    Assert.assertEquals("B", copy.getHeader().getAttribute("b"));
    copy.getHeader().setAttribute("a", "AAA");
    copy.getHeader().setAttribute("b", "BB");
    Assert.assertEquals("AA", record.getHeader().getAttribute("a"));
    Assert.assertEquals("AAA", copy.getHeader().getAttribute("a"));
    copy.getHeader().setAttribute("b", "BB");
    Assert.assertEquals("B", record.getHeader().getAttribute("b"));
    Assert.assertEquals("BB", copy.getHeader().getAttribute("b"));
  }

  @Test
  public void testRootBasicField() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);

    // no root field
    Assert.assertNull(r.get());
    Assert.assertNull(r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.getFieldPaths().isEmpty());
    Assert.assertNull(r.delete(""));
    Assert.assertNull(r.delete("/a"));
    Assert.assertNull(r.delete("[1]"));
    Assert.assertFalse(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));

    Field f = Field.create(true);
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertEquals(f, r.delete(""));
    Assert.assertFalse(r.has(""));
    Assert.assertNull(r.delete("/a"));
    Assert.assertNull(r.delete("[1]"));
  }

  @Test
  public void testRootMapField() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);

    Field f = Field.create(Field.Type.MAP, null);
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertEquals(f, r.delete(""));
    Assert.assertNull(r.get());

    f = Field.create(new HashMap<String, Field>());
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertEquals(f, r.delete(""));
    Assert.assertNull(r.get());

    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(true));
    f = Field.create(Field.Type.MAP, map);
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertEquals(Field.create(true), r.get("/a"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertTrue(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertEquals(ImmutableSet.of("", "/a"), r.getFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("/a"));
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertEquals(f, r.delete(""));
    Assert.assertNull(r.get());
  }

  @Test
  public void testRootListField() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);

    Field f = Field.create(Field.Type.LIST, null);
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertEquals(f, r.delete(""));
    Assert.assertNull(r.get());

    f = Field.create(new ArrayList<Field>());
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertNull(r.get("/b"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("/b"));
    Assert.assertFalse(r.has("[0]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertEquals(f, r.delete(""));
    Assert.assertNull(r.get());

    List<Field> list = new ArrayList<>();
    list.add(Field.create(true));
    f = Field.create(list);
    r.set(f);
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertEquals(Field.create(true), r.get("[0]"));
    Assert.assertNull(r.get("[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertTrue(r.has("[0]"));
    Assert.assertFalse(r.has("/a"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertEquals(ImmutableSet.of("", "[0]"), r.getFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("[0]"));
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertFalse(r.has("[0]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getFieldPaths());
    Assert.assertEquals(f, r.delete(""));
    Assert.assertNull(r.get());
  }

  @Test
  public void testMapListField() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    List<Field> list = new ArrayList<>();
    list.add(Field.create(true));
    Field listField = Field.create(list);
    Map<String, Field> map = new HashMap<>();
    map.put("a", listField);
    Field mapField = Field.create(map);
    r.set(mapField);

    Assert.assertEquals(mapField, r.get());
    Assert.assertEquals(mapField, r.get(""));
    Assert.assertEquals(listField, r.get("/a"));
    Assert.assertEquals(Field.create(true), r.get("/a[0]"));
    Assert.assertNull(r.get("/a[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertTrue(r.has("/a"));
    Assert.assertFalse(r.has("/b"));
    Assert.assertTrue(r.has("/a[0]"));
    Assert.assertFalse(r.has("/a[1]"));
    Assert.assertEquals(ImmutableSet.of("", "/a", "/a[0]"), r.getFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("/a[0]"));
    Assert.assertEquals(ImmutableSet.of("", "/a"), r.getFieldPaths());
    Assert.assertEquals(Field.create(new ArrayList<Field>()), r.delete("/a"));
  }

  @Test
  public void testListMapField() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(true));
    Field mapField = Field.create(map);
    List<Field> list = new ArrayList<>();
    list.add(mapField);
    Field listField = Field.create(list);
    r.set(listField);

    Assert.assertEquals(listField, r.get());
    Assert.assertEquals(listField, r.get(""));
    Assert.assertEquals(mapField, r.get("[0]"));
    Assert.assertEquals(Field.create(true), r.get("[0]/a"));
    Assert.assertTrue(r.has(""));
    Assert.assertTrue(r.has("[0]"));
    Assert.assertFalse(r.has("[1]"));
    Assert.assertTrue(r.has("[0]/a"));
    Assert.assertFalse(r.has("[1]/a"));
    Assert.assertEquals(ImmutableSet.of("", "[0]", "[0]/a"), r.getFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("[0]/a"));
    Assert.assertEquals(ImmutableSet.of("", "[0]"), r.getFieldPaths());
    Assert.assertEquals(Field.create(new HashMap<String, Field>()), r.delete("[0]"));
  }

}
