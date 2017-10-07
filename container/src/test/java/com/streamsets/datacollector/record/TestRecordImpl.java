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
package com.streamsets.datacollector.record;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    Assert.assertTrue(r.getEscapedFieldPaths().isEmpty());
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
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of("", "/a"), r.getEscapedFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("/a"));
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertNull(r.get("/a"));
    Assert.assertTrue(r.has(""));
    Assert.assertFalse(r.has("/a"));
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of("", "[0]"), r.getEscapedFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("[0]"));
    Assert.assertEquals(f, r.get());
    Assert.assertEquals(f, r.get(""));
    Assert.assertFalse(r.has("[0]"));
    Assert.assertEquals(ImmutableSet.of(""), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of("", "/a", "/a[0]"), r.getEscapedFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("/a[0]"));
    Assert.assertEquals(ImmutableSet.of("", "/a"), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of("", "[0]", "[0]/a"), r.getEscapedFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("[0]/a"));
    Assert.assertEquals(ImmutableSet.of("", "[0]"), r.getEscapedFieldPaths());
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
    Assert.assertEquals(ImmutableSet.of("", "/'a" + escaped + "'", "/'a" + escaped + "'[0]"), r.getEscapedFieldPaths());
    Assert.assertEquals(Field.create(true), r.delete("/a" + escaped + "[0]"));
    Assert.assertEquals(ImmutableSet.of("", "/'a" + escaped + "'"), r.getEscapedFieldPaths());
    Assert.assertEquals(Field.create(new ArrayList<Field>()), r.delete("/a" + escaped));
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

  @Test(expected = IllegalArgumentException.class)
  public void testAddAPINonListNonMapParent() {
    //record with boolean field at the root
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    r.set(Field.create(true));
    r.set("/a", Field.create(false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFieldPathNotReachable() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    r.set(Field.create(new HashMap<String, Field>()));
    r.set("/a[1]", Field.create(false));
  }

  public void testEscapingWithQuotes(String specialChar, String pathSpecialChar) {
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
    Assert.assertEquals(listField, r.get("/'a" + pathSpecialChar + "'"));
    Assert.assertEquals(Field.create(true), r.get("/'a" + pathSpecialChar + "'[0]"));
    Assert.assertNull(r.get("/'a" + pathSpecialChar + "'[1]"));
    Assert.assertTrue(r.has(""));
    Assert.assertTrue(r.has("/'a" + pathSpecialChar + "'"));
    Assert.assertFalse(r.has("/b"));
    Assert.assertTrue(r.has("/'a" + pathSpecialChar + "'[0]"));
    Assert.assertFalse(r.has("/'a" + pathSpecialChar + "'[1]"));
    Assert.assertEquals(Field.create(true), r.delete("/'a" + pathSpecialChar + "'[0]"));
    Assert.assertEquals(Field.create(new ArrayList<Field>()), r.delete("/'a" + pathSpecialChar + "'"));
  }

  @Test
  public void testEscapingWithQuotesSlash() {
    testEscapingWithQuotes("/", "/");
  }

  @Test
  public void testEscapingWithQuotesOpenBracket() {
    testEscapingWithQuotes("[", "[");
  }

  @Test
  public void testEscapingWithQuotesCloseBracket() {
    testEscapingWithQuotes("[", "[");
  }

  @Test
  public void testEscapingWithQuotesEscapeQuote() {
    testEscapingWithQuotes("'", "\\'");
  }

  @Test
  public void testEscapingWitMixed() {
    testEscapingWithQuotes(" & [] / - DF [ & ' ", " & [] / - DF [ & \\' ");
  }


  @Test
  public void testQuoteEscaping() {
    Map<String, Field> map;

    RecordImpl r = new RecordImpl("stage", "source", null, null);

    map = new HashMap<>();
    map.put("foo", Field.create("fooValue"));
    map.put("'foo", Field.create("quoteFooValue"));
    map.put("foo bar", Field.create("foo space bar Value"));
    map.put("foo\bar", Field.create("foo back slash Value"));
    map.put("foo bar list", Field.create(ImmutableList.of(Field.create("foo space bar list1"),
      Field.create("foo space bar list2"))));
    map.put("foo\"bar", Field.create("foo double quote bar Value"));
    map.put("foo'bar", Field.create("foo single quote bar Value"));

    map.put("fooList[1]", Field.create("foo List Like Value"));
    List<Field> list = new ArrayList<>();
    list.add(Field.create("list1"));
    list.add(Field.create("list2"));
    Field listField = Field.create(list);
    map.put("fooList", listField);
    map.put("fooMap/bar", Field.create("foo Map Like Value"));

    Map<String, Field> nestedMap = new HashMap<>();
    nestedMap.put("bar", Field.create("nested map bar value"));
    map.put("fooMap", Field.create(nestedMap));

    Field mapField = Field.create(map);
    r.set(mapField);

    Assert.assertEquals("fooValue", r.get("/foo").getValue());
    Assert.assertEquals("fooValue", r.get("/'foo'").getValue());
    Assert.assertEquals("quoteFooValue", r.get("/'\\'foo'").getValue());
    Assert.assertEquals("foo space bar Value", r.get("/foo bar").getValue());
    Assert.assertEquals("foo space bar Value", r.get("/'foo bar'").getValue());

    Assert.assertEquals("foo back slash Value", r.get("/'foo\bar'").getValue());

    Assert.assertEquals("foo space bar list1", r.get("/foo bar list[0]").getValue());
    Assert.assertEquals("foo space bar list1", r.get("/'foo bar list'[0]").getValue());
    Assert.assertEquals("foo double quote bar Value", r.get("/'foo\"bar'").getValue());
    Assert.assertEquals("foo single quote bar Value", r.get("/\"foo'bar\"").getValue());
    Assert.assertEquals("foo List Like Value", r.get("/'fooList[1]'").getValue());
    Assert.assertEquals("list2", r.get("/fooList[1]").getValue());
    Assert.assertEquals("foo Map Like Value", r.get("/'fooMap/bar'").getValue());
    Assert.assertEquals("nested map bar value", r.get("/fooMap/bar").getValue());
  }

  @Test
  public void testListMapRecord() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
    listMap.put("A", Field.create("ALPHA"));
    listMap.put("B", Field.create("BETA"));
    listMap.put("G", Field.create("GAMMA"));
    Field listMapField = Field.createListMap(listMap);
    r.set(listMapField);

    Assert.assertEquals("ALPHA", r.get("/A").getValue());
    Assert.assertEquals("ALPHA", r.get("[0]").getValue());
    Assert.assertEquals("BETA", r.get("/B").getValue());
    Assert.assertEquals("BETA", r.get("[1]").getValue());
    Assert.assertEquals("GAMMA", r.get("/G").getValue());
    Assert.assertEquals("GAMMA", r.get("[2]").getValue());
  }

  // Validate that all paths returned by getEscapedFieldPaths are in fact reachable
  @Test
  public void testEscapeAndRetrieveSpecialChars() {
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    LinkedHashMap<String, Field> rootMap = new LinkedHashMap<>();

    LinkedHashMap<String, Field> innerNonNesterMap = new LinkedHashMap<>();
    innerNonNesterMap.put("a/b", Field.create("value"));
    innerNonNesterMap.put("a[b", Field.create("value"));
    innerNonNesterMap.put("a]b", Field.create("value"));
    innerNonNesterMap.put("a'b", Field.create("value"));
    innerNonNesterMap.put("a\\b", Field.create("value"));
    innerNonNesterMap.put("a\"b", Field.create("value"));

    //backslash and quotes combined
    innerNonNesterMap.put("a\\\"b", Field.create("value"));
    innerNonNesterMap.put("a\\'b", Field.create("value"));

    innerNonNesterMap.put("a\"\\b", Field.create("value"));
    innerNonNesterMap.put("a'\\b", Field.create("value"));

    innerNonNesterMap.put("a\\\'b", Field.create("value"));
    innerNonNesterMap.put("a\'\\b", Field.create("value"));

    innerNonNesterMap.put("a\\\"'b", Field.create("value"));
    innerNonNesterMap.put("a\\'\"b", Field.create("value"));

    innerNonNesterMap.put("a\\\'\"b", Field.create("value"));
    innerNonNesterMap.put("a\\'\"b", Field.create("value"));

    //multiple quotes and backslash combined
    innerNonNesterMap.put("a\\\\'\\\"\"\\\\b", Field.create("value"));

    //Single level map (ex: /innerNonNesterMap/'a\'b')
    rootMap.put("innerNonNesterMap", Field.createListMap(innerNonNesterMap));

    //Nested map (ex: /innerNestedMap/'a\'b'/'a\'b')
    LinkedHashMap<String, Field> innerNestedMap = new LinkedHashMap<>();
    for (String key : innerNonNesterMap.keySet()) {
      innerNestedMap.put(key, Field.createListMap(innerNonNesterMap));
    }
    rootMap.put("innerNestedListMap", Field.createListMap(innerNestedMap));

    //Two map fields in a list (one single level and one nested from above)
    List<Field> innerList = new ArrayList<>();
    innerList.add(Field.createListMap(innerNonNesterMap));
    innerList.add(Field.createListMap(innerNestedMap));
    rootMap.put("innerList", Field.create(innerList));

    r.set(Field.createListMap(rootMap));

    Set<String> paths = r.getEscapedFieldPaths();
    for(String path : paths) {
      // Skip implied root
      if("".equals(path)) {
        continue;
      }

      Assert.assertTrue("Path " + path + " does not exist", r.has(path));
      Field f = r.get(path);
      if (f.getType() == Field.Type.STRING) {
        Assert.assertEquals("value", f.getValueAsString());
      }
    }
  }
}
