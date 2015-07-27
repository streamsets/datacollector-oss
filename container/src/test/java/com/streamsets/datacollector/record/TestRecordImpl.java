/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.record;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRecordImpl {

  @Test(expected = NullPointerException.class)
  public void testConstructorInvalid1() {
    new RecordImpl(null, (String) null, null, null);
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
    Assert.assertNull(header.getStagesPath());

    record.addStageToStagePath("x");
    Assert.assertEquals("x", header.getStagesPath());

    record.addStageToStagePath("y");
    Assert.assertEquals("x:y", header.getStagesPath());

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

  @Test
  public void testClone() {
    RecordImpl record = new RecordImpl("stage", "source", null, null);
    record.set(Field.create(new HashMap<String, Field>()));
    record.getHeader().setAttribute("a", "A");
    RecordImpl clone = record.clone();
    Assert.assertEquals(clone, record);
    Assert.assertNotSame(clone, record);
  }

  // tests for field-path expressions

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

  public void testEscaping(char specialChar) {
    String escaped = "" + specialChar + specialChar;
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    List<Field> list = new ArrayList<>();
    list.add(Field.create(true));
    Field listField = Field.create(list);
    Map<String, Field> map = new HashMap<>();
    map.put("a" + specialChar, listField);
    Field mapField = Field.create(map);
    r.set(mapField);

    Assert.assertEquals(mapField, r.get());
    Assert.assertEquals(mapField, r.get(""));
    Assert.assertEquals(listField, r.get("/a" + escaped));
    Assert.assertEquals(Field.create(true), r.get("/a" + escaped + "[0]"));
    Assert.assertNull(r.get("/a" + escaped + "[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertTrue(r.has("/a" + escaped));
    Assert.assertFalse(r.has("/b"));
    Assert.assertTrue(r.has("/a" + escaped + "[0]"));
    Assert.assertFalse(r.has("/a" + escaped + "[1]"));
    Assert.assertEquals(ImmutableSet.of("", "/a" + escaped, "/a" + escaped + "[0]"), r.getFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("/a" + escaped + "[0]"));
    Assert.assertEquals(ImmutableSet.of("", "/a" + escaped), r.getFieldPaths());
    Assert.assertEquals(Field.create(new ArrayList<Field>()), r.delete("/a" + escaped));
  }

  @Test
  public void testEscapingSlash() {
    testEscaping('/');
  }

  @Test
  public void testEscapingOpenBracket() {
    testEscaping('[');
  }

  @Test
  public void testEscapingCloseBracket() {
    testEscaping(']');
  }

  @Test
  public void testSetInMap() {
    // Root field is the list
    // "[0]" is the map containing a boolean "true" with key a
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    Map<String, Field> map = new HashMap<>();
    map.put("a", Field.create(true));
    Field mapField = Field.create(map);
    List<Field> list = new ArrayList<>();
    list.add(mapField);
    Field listField = Field.create(list);
    r.set(listField);

    //add boolean "false" in the map [0] with key "b"
    r.set("[0]/b", Field.create(false));
    Assert.assertTrue(r.has("[0]/b"));
    Assert.assertEquals(false, r.get("[0]/b").getValueAsBoolean());

    //add boolean "true" in the map [0] with key "b". It should replace old value
    r.set("[0]/b", Field.create(true));
    Assert.assertTrue(r.has("[0]/b"));
    Assert.assertEquals(true, r.get("[0]/b").getValueAsBoolean());

    r.set("[0]/c", Field.create("Hello world"));
    Assert.assertTrue(r.has("[0]/c"));
    Assert.assertEquals("Hello world", r.get("[0]/c").getValueAsString());

    try {
      r.set("[0]/c/d", Field.create("Hello world"));
      Assert.fail("IllegalArgumentException expected as type of [0]/c is not map");
    } catch (IllegalArgumentException e) {

    }

  }

  @Test
  public void testSetInList() {
    // Root field is the list
    // "[0]" is a boolean true
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    List<Field> list = new ArrayList<>();
    list.add(Field.create(true));
    Field listField = Field.create(list);
    r.set(listField);

    //add element to list, index == current size of list
    r.set("[1]", Field.create(false));
    Assert.assertTrue(r.has("[1]"));
    Assert.assertEquals(false, r.get("[1]").getValueAsBoolean());

    //replace element in list
    r.set("[1]", Field.create(true));
    Assert.assertTrue(r.has("[1]"));
    Assert.assertEquals(true, r.get("[1]").getValueAsBoolean());

    //ensure no insert
    Assert.assertFalse(r.has("[2]"));

    //add element to list, index == current size of list
    r.set("[2]", Field.create(false));
    Assert.assertTrue(r.has("[2]"));
    Assert.assertEquals(false, r.get("[2]").getValueAsBoolean());

    try {
      r.set("[8]", Field.create(true));
      Assert.fail("Expected IndexOutOfBoundsException as the list contains only 3 elements");
    } catch (IndexOutOfBoundsException e) {

    }

    try {
      r.set("[2]/c", Field.create("Hello world"));
      Assert.fail("IllegalArgumentException expected as type of [2] is not map");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddAPINonListNonMapParent() {
    //record with boolean field at the root
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    r.set(Field.create(true));

    try {
      r.set("/a", Field.create(false));
      Assert.fail("Expected IllegalArgumentException as the root field is not map or list");
    } catch (IllegalArgumentException e) {

    }
  }

}
