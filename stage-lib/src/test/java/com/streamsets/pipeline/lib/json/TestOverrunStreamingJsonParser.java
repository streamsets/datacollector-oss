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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.lib.io.OverrunException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class TestOverrunStreamingJsonParser {

  @Before
  public void setUp() {
    System.getProperties().remove(OverrunStreamingJsonParser.OVERRUN_LIMIT_SYS_PROP);
  }

  @After
  public void cleanUp() {
    setUp();
  }

  private Reader getJsonReader(String name) throws Exception {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    return (is != null) ? new InputStreamReader(is) : null;
  }

  // Parser level overrun, Array

  @Test
  public void testArrayOfObjects() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
        getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"), StreamingJsonParser.Mode.ARRAY_OBJECTS,
        50);
    List a1 = parser.readList();
    Assert.assertNotNull(a1);
    try {
      parser.readList();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List a2 = parser.readList();
    Assert.assertNotNull(a2);
    Assert.assertNull(parser.readList());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  // Parser level overrun, Objects

  @Test
  public void testMultipleObjects() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
        getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"), StreamingJsonParser.Mode.MULTIPLE_OBJECTS,
        50);
    List a1 = parser.readList();
    Assert.assertNotNull(a1);
    try {
      parser.readList();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List a2 = parser.readList();
    Assert.assertNotNull(a2);
    Assert.assertNull(parser.readList());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  // Stream level overrun, Array

  public void testStreamLevelOverrunArray(boolean attemptNextRead) throws Exception {
    System.setProperty(OverrunStreamingJsonParser.OVERRUN_LIMIT_SYS_PROP, "10000");
    String json = "[[\"a\"],[\"" + Strings.repeat("a", 20000) + "\"],[\"b\"]]";
    StreamingJsonParser parser = new OverrunStreamingJsonParser(new StringReader(json),
                                                                StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    List a1 = parser.readList();
    Assert.assertNotNull(a1);
    Assert.assertEquals(ImmutableList.of("a"), a1);

    if (!attemptNextRead) {
      parser.readList();
    } else {
      try {
        parser.readList();
      } catch (OverrunException ex) {
        //NOP
      }
      parser.readList();
    }
  }

  @Test(expected = OverrunException.class)
  public void testStreamLevelOverrunArray() throws Exception {
    testStreamLevelOverrunArray(false);
  }

  @Test(expected = IllegalStateException.class)
  public void testStreamLevelOverrunArrayAttemptNextRead() throws Exception {
    testStreamLevelOverrunArray(true);
  }

  // Stream level overrun, Object

  public void testStreamLevelOverrunMultipleObjects(boolean attemptNextRead) throws Exception {
    System.setProperty(OverrunStreamingJsonParser.OVERRUN_LIMIT_SYS_PROP, "10000");
    String json = "[\"a\"][\"" + Strings.repeat("a", 20000) + "\"][\"b\"]";
    StreamingJsonParser parser = new OverrunStreamingJsonParser(new StringReader(json),
                                                                StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    List a1 = parser.readList();
    Assert.assertNotNull(a1);
    Assert.assertEquals(ImmutableList.of("a"), a1);

    if (!attemptNextRead) {
      parser.readList();
    } else {
      try {
        parser.readList();
      } catch (OverrunException ex) {
        //NOP
      }
      parser.readList();
    }
  }

  @Test(expected = OverrunException.class)
  public void testStreamLevelOverrunMultipleObjects() throws Exception {
    testStreamLevelOverrunMultipleObjects(false);
  }

  @Test(expected = IllegalStateException.class)
  public void testStreamLevelOverrunMultipleObjectsAttemptNextRead() throws Exception {
    testStreamLevelOverrunMultipleObjects(true);
  }

  @Test
  public void testArrayPositionable() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
      getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"), StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    List l1 = parser.readList();
    long firstObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);
    try {
      parser.readList();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2 = parser.readList();
    long secondObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);
    Assert.assertNotNull(l2);
    long lastObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);
    Assert.assertNull(parser.readList());
    long endPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            firstObjectPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    try {
      parser.readList();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2a = parser.readList();
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            secondObjectPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            lastObjectPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            endPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(endPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());
    Assert.assertEquals(endPos, parser.getReaderPosition());
  }

  @Test
  public void testMultipleObjectsPositionable() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
        getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"), StreamingJsonParser.Mode.MULTIPLE_OBJECTS,
        50);
    List l1 = parser.readList();
    long firstObjectPos = parser.getReaderPosition();    Assert.assertNotNull(l1);
    try {
      parser.readList();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2 = parser.readList();
    long secondObjectPos = parser.getReaderPosition();    Assert.assertNotNull(l1);
    Assert.assertNotNull(l2);
    long lastObjectPos = parser.getReaderPosition();    Assert.assertNotNull(l1);

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"),
                                            firstObjectPos, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    try {
      parser.readList();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2a = parser.readList();
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"),
                                            secondObjectPos, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"),
                                            lastObjectPos, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull(parser.readList());
  }

}
