/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.json;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.OverrunException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.List;

public class TestOverrunStreamingJsonParser {

  @Before
  public void setUp() {
    System.getProperties().remove(OverrunStreamingJsonParser.OVERRUN_LIMIT_SYS_PROP);
  }

  @After
  public void cleanUp() {
    setUp();
  }

  private CountingReader getJsonReader(String name) throws Exception {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    return (is != null) ? new CountingReader(new InputStreamReader(is)) : null;
  }

  // Parser level overrun, Array

  @Test
  public void testArrayOfObjects() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
        getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"), StreamingJsonParser.Mode.ARRAY_OBJECTS,
        50);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  // Parser level overrun, Objects

  @Test
  public void testMultipleObjects() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
        getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"), StreamingJsonParser.Mode.MULTIPLE_OBJECTS,
        50);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List a2 = (List) parser.read();
    Assert.assertNotNull(a2);
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(ImmutableList.of("a", "A"), a1);
    Assert.assertEquals(ImmutableList.of("b", "B"), a2);
  }

  // Stream level overrun, Array

  public void testStreamLevelOverrunArray(boolean attemptNextRead) throws Exception {
    System.setProperty(OverrunStreamingJsonParser.OVERRUN_LIMIT_SYS_PROP, "10000");
    String json = "[[\"a\"],[\"" + Strings.repeat("a", 20000) + "\"],[\"b\"]]";
    StreamingJsonParser parser = new OverrunStreamingJsonParser(new CountingReader(new StringReader(json)),
                                                                StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    Assert.assertEquals(ImmutableList.of("a"), a1);

    if (!attemptNextRead) {
      parser.read();
    } else {
      try {
        parser.read();
      } catch (OverrunException ex) {
        //NOP
      }
      parser.read();
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
    StreamingJsonParser parser = new OverrunStreamingJsonParser(new CountingReader(new StringReader(json)),
                                                                StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    List a1 = (List) parser.read();
    Assert.assertNotNull(a1);
    Assert.assertEquals(ImmutableList.of("a"), a1);

    if (!attemptNextRead) {
      parser.read();
    } else {
      try {
        parser.read();
      } catch (OverrunException ex) {
        //NOP
      }
      parser.read();
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
    List l1 = (List) parser.read();
    long firstObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2 = (List) parser.read();
    long secondObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);
    Assert.assertNotNull(l2);
    long lastObjectPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);
    Assert.assertNull((List) parser.read());
    long endPos = parser.getReaderPosition();
    Assert.assertNotNull(l1);

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            firstObjectPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2a = (List) parser.read();
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            secondObjectPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            lastObjectPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-arrayOfObjects.json"),
                                            endPos, StreamingJsonParser.Mode.ARRAY_OBJECTS, 50);
    Assert.assertEquals(endPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());
    Assert.assertEquals(endPos, parser.getReaderPosition());
  }

  @Test
  public void testMultipleObjectsPositionable() throws Exception {
    StreamingJsonParser parser = new OverrunStreamingJsonParser(
        getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"), StreamingJsonParser.Mode.MULTIPLE_OBJECTS,
        50);
    List l1 = (List) parser.read();
    long firstObjectPos = parser.getReaderPosition();    Assert.assertNotNull(l1);
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2 = (List) parser.read();
    long secondObjectPos = parser.getReaderPosition();    Assert.assertNotNull(l1);
    Assert.assertNotNull(l2);
    long lastObjectPos = parser.getReaderPosition();    Assert.assertNotNull(l1);

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"),
                                            firstObjectPos, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    Assert.assertEquals(firstObjectPos, parser.getReaderPosition());
    try {
      parser.read();
      Assert.fail();
    } catch (OverrunStreamingJsonParser.JsonObjectLengthException ex) {
      //NOP
    }
    List l2a = (List) parser.read();
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"),
                                            secondObjectPos, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    Assert.assertEquals(secondObjectPos, parser.getReaderPosition());
    Assert.assertEquals(l2, l2a);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());

    parser = new OverrunStreamingJsonParser(getJsonReader("TestOverrunStreamingJsonParser-multipleObjects.json"),
                                            lastObjectPos, StreamingJsonParser.Mode.MULTIPLE_OBJECTS, 50);
    Assert.assertEquals(lastObjectPos, parser.getReaderPosition());
    Assert.assertNull((List) parser.read());
  }

}
