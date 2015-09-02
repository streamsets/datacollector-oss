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
package com.streamsets.pipeline.lib.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class TestStreamingJsonParser {

  private Reader getJsonReader(String name) throws Exception {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    return (is != null) ? new InputStreamReader(is) : null;
  }

  @Test
  public void testIncorrectJSONMode() throws Exception {
    Reader reader = new InputStreamReader(new OverrunInputStream(Thread.currentThread().getContextClassLoader()
      .getResourceAsStream("TestStreamingJsonParser-testIncorrectJSONMode.json"), 128, true));
    StreamingJsonParser parser = new StreamingJsonParser(reader,
      StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunException e) {
      Assert.assertEquals(164, e.getStreamOffset());
    }
  }

  // Array of Maps

  @Test
  public void testArrayOfMaps() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Map m1 = (Map) parser.read();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    Assert.assertNotNull(m2);
    Assert.assertNull((Map) parser.read());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  @Test
  public void testArrayOfMapsUsingRead() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Map m1 = (Map) parser.read();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    Assert.assertNotNull(m2);
    Assert.assertNull(parser.read());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  // Array of arrays

  @Test
  public void testArrayOfArrays() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfArrays.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  @Test
  public void testArrayOfArraysUsingRead() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfArrays.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  // Multiple Maps

  @Test
  public void testMultipleMaps() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Map m1 = (Map) parser.read();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    Assert.assertNotNull(m2);
    Assert.assertNull((Map) parser.read());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  @Test
  public void testMultipleMapsUsingRead() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Map m1 = (Map) parser.read();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    Assert.assertNotNull(m2);
    Assert.assertNull((Map)parser.read());
    Assert.assertEquals(ImmutableMap.of("a", "A"), m1);
    Assert.assertEquals(ImmutableMap.of("b", "B"), m2);
  }

  // Multiple array

  @Test
  public void testMultipleArrays() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleArrays.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  @Test
  public void testMultipleArraysUsingRead() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleArrays.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  @Test
  public void testArrayPositionable() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"),
                                                         StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Map m1 = (Map) parser.read();
    long firstObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    long secondObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Assert.assertNotNull(m2);
    long lastObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Assert.assertNull((Map) parser.read());
    long endPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), firstObjectPos,
                                     StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    Map m2a = (Map) parser.read();
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), secondObjectPos,
                                     StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), lastObjectPos,
                                     StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-arrayOfMaps.json"), endPos,
                                     StreamingJsonParser.Mode.ARRAY_OBJECTS);
    Assert.assertEquals(endPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());
  }

  @Test
  public void testMultipleObjectsPositionable() throws Exception {
    StreamingJsonParser parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleMaps.json"),
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Map m1 = (Map) parser.read();
    long firstObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Map m2 = (Map) parser.read();
    long secondObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);
    Assert.assertNotNull(m2);
    long lastObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(m1);

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleMaps.json"), firstObjectPos,
                                                         StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    Map m2a = (Map) parser.read();
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleMaps.json"), secondObjectPos,
                                     StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(m2, m2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());

    parser = new StreamingJsonParser(getJsonReader("TestStreamingJsonParser-multipleMaps.json"), lastObjectPos,
                                     StreamingJsonParser.Mode.MULTIPLE_OBJECTS);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((Map) parser.read());
  }

}
