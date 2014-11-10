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

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

public class TestRecordImpl {

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid1() {
    new RecordImpl(null, null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid2() {
    new RecordImpl("s", null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid3() {
    new RecordImpl(null, "s", null, null);
  }

  @Test
  public void testFieldMethods() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    Assert.assertFalse(record.getFieldNames().hasNext());
    try {
      record.setField("a", null);
      Assert.fail();
    } catch (NullPointerException ex) {
      //expected
    }
    Assert.assertFalse(record.getFieldNames().hasNext());
    Assert.assertTrue(record.getValues().isEmpty());
    Field f =  Field.create(true);
    record.setField("a", f);
    Assert.assertTrue(record.getFieldNames().hasNext());
    Iterator<String> it = record.getFieldNames();
    Assert.assertEquals("a", it.next());
    Assert.assertFalse(it.hasNext());
    Assert.assertEquals(f, record.getField("a"));
    Assert.assertEquals(1, record.getValues().size());
    Assert.assertEquals(f, record.getValues().get("a"));
    record.deleteField("a");
    Assert.assertNull(record.getField("a"));
    Assert.assertFalse(record.getFieldNames().hasNext());
    Assert.assertTrue(record.getValues().isEmpty());
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

    Assert.assertFalse(header.getAttributeNames().hasNext());
    try {
      header.setAttribute("a", null);
      Assert.fail();
    } catch (NullPointerException ex) {
      //expected
    }
    Assert.assertFalse(header.getAttributeNames().hasNext());
    RecordImpl.HeaderImpl headerImpl = ((RecordImpl.HeaderImpl)header);
    Assert.assertTrue(headerImpl.getValues().isEmpty());
    header.setAttribute("a", "A");
    Assert.assertTrue(header.getAttributeNames().hasNext());
    Iterator<String> it = header.getAttributeNames();
    Assert.assertEquals("a", it.next());
    Assert.assertFalse(it.hasNext());
    Assert.assertEquals("A", header.getAttribute("a"));
    Assert.assertEquals(1, headerImpl.getValues().size());
    Assert.assertEquals("A", headerImpl.getValues().get("a"));
    header.deleteAttribute("a");
    Assert.assertNull(header.getAttribute("a"));
    Assert.assertFalse(header.getAttributeNames().hasNext());
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
    Set<String> sFields = Sets.newHashSet(Iterators.filter(snapshot.getFieldNames(), String.class));
    Set<String> oFields = Sets.newHashSet(Iterators.filter(original.getFieldNames(), String.class));
    Assert.assertEquals(sFields, oFields);
    for (String name : sFields) {
      Assert.assertEquals(snapshot.getField(name), original.getField(name));
    }
    Set<String> sAttrs = Sets.newHashSet(Iterators.filter(snapshot.getHeader().getAttributeNames(), String.class));
    Set<String> oAttrs = Sets.newHashSet(Iterators.filter(original.getHeader().getAttributeNames(), String.class));
    Assert.assertEquals(sAttrs, oAttrs);
    for (String name : sAttrs) {
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
    Set<String> sFields = Sets.newHashSet(Iterators.filter(copy.getFieldNames(), String.class));
    Set<String> oFields = Sets.newHashSet(Iterators.filter(original.getFieldNames(), String.class));
    Assert.assertEquals(sFields, oFields);
    for (String name : sFields) {
      Assert.assertEquals(copy.getField(name), original.getField(name));
    }
    Set<String> sAttrs = Sets.newHashSet(Iterators.filter(copy.getHeader().getAttributeNames(), String.class));
    Set<String> oAttrs = Sets.newHashSet(Iterators.filter(original.getHeader().getAttributeNames(), String.class));
    Assert.assertEquals(sAttrs, oAttrs);
    for (String name : sAttrs) {
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
    record.setField("a", Field.create(true));
    record.getHeader().setAttribute("a", "A");
    RecordImpl snapshot = record.createSnapshot();
    assertIsSnapshot(record, snapshot);
  }

  @Test
  public void testAssertIsCopy() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.setField("a", Field.create(true));
    record.getHeader().setAttribute("a", "A");
    RecordImpl copy = record.createCopy();
    assertIsCopy(record, copy, true);
  }

  @Test
  public void testAssertIsCopyDontCompareStages() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.setField("a", Field.create(true));
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

}
