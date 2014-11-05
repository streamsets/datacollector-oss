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

import java.util.Set;
import java.util.UUID;

public class TestRecordImpl {

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
      Assert.assertEquals(snapshot.getHeader().getCreatorStage(), original.getHeader().getCreatorStage());
      Assert.assertEquals(snapshot.getHeader().getRaw(), original.getHeader().getRaw());
      Assert.assertEquals(snapshot.getHeader().getRawMime(), original.getHeader().getRawMime());
      Assert.assertEquals(snapshot.getHeader().getRecordSourceId(), original.getHeader().getRecordSourceId());
    } finally {
      snapshot.getHeader().removeAttribute(randomKey);
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
      copy.getHeader().removeAttribute(randomKey);
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
