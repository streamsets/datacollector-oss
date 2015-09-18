/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.io;

import com.google.common.base.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;

public class TestOverrunReader {

  @Test
  public void testOverrunUnderLimit() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64, true, false);
    char[] buff = new char[128];
    ois.read(buff, 0, 64);
    ois.resetCount();
    ois.read(buff, 0, 64);
  }

  @Test(expected = OverrunException.class)
  public void testOverrunOverLimit() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64, true, false);
    char[] buff = new char[128];
    ois.read(buff, 0, 65);
  }

  @Test
  public void testOverrunOverLimitNotEnabled() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 128));
    OverrunReader ois = new OverrunReader(is, 64, false, false);
    char[] buff = new char[128];
    ois.read(buff, 0, 65);
  }

  @Test(expected =  OverrunException.class)
  public void testOverrunOverLimitPostConstructorEnabled() throws Exception {
    Reader is = new StringReader(Strings.repeat("a", 1280));
    OverrunReader ois = new OverrunReader(is, 64, false, false);
    char[] buff = new char[128];
    try {
      ois.read(buff, 0, 65);
    } catch (OverrunException ex) {
      Assert.fail();
    }
    ois.setEnabled(true);
    buff = new char[128];
    ois.read(buff, 0, 65);
  }

  @Test
  public void testIsControl() {
    char[] ctrl = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
        28, 29, 30, 31, 127};
    char[] noCtrl = { 9, 10, 13, 32, 126, 128 };
    for (char c : ctrl) {
      Assert.assertTrue("control char: " + (int) c, OverrunReader.isControl(c));
    }
    for (char c : noCtrl) {
      Assert.assertFalse("no control char: " + (int) c, OverrunReader.isControl(c));
    }
  }

  @Test
  public void testFindFirstControlIdx() {
    Assert.assertEquals( -1, OverrunReader.findFirstControlIdx(new char[]{}, 0, 0));
    Assert.assertEquals( -1, OverrunReader.findFirstControlIdx(new char[]{ 32 }, 0, 1));
    Assert.assertEquals( 0, OverrunReader.findFirstControlIdx(new char[]{ 0 }, 0, 1));
    Assert.assertEquals( 0, OverrunReader.findFirstControlIdx(new char[]{ 0, 32 }, 0, 2));
    Assert.assertEquals( -1, OverrunReader.findFirstControlIdx(new char[]{ 0, 32 }, 1, 2));
    Assert.assertEquals( 0, OverrunReader.findFirstControlIdx(new char[]{ 0, 32, 0 }, 0, 3));
    Assert.assertEquals( 2, OverrunReader.findFirstControlIdx(new char[]{ 0, 32, 0 }, 1, 3));
    Assert.assertEquals( -1, OverrunReader.findFirstControlIdx(new char[]{ 0, 32, 0 }, 3, 3));
  }

  @Test
  public void testRemoveControlChars() {
    char[] original;
    char[] expected;
    char[] got;

    original = new char[] { };
    got = new char[0];
    expected = original;
    Assert.assertEquals(0, OverrunReader.removeControlChars(original, 0, got, 0));
    Assert.assertArrayEquals(expected, got);

    original = new char[] { 32 };
    got = new char[1];
    expected = original;
    Assert.assertEquals(1, OverrunReader.removeControlChars(original, 1, got, 0));
    Assert.assertArrayEquals(expected, got);

    original = new char[] { 32, 33 };
    got = new char[10];
    Assert.assertEquals(2, OverrunReader.removeControlChars(original, 2, got, 2));
    Assert.assertEquals((char)32, got[2]);
    Assert.assertEquals((char)33, got[3]);

    original = new char[] { 0, 32, 0, 33, 0 };
    got = new char[2];
    expected = new char[] { 32, 33 };
    Assert.assertEquals(2, OverrunReader.removeControlChars(original, 5, got, 0));
    Assert.assertArrayEquals(expected, got);
  }

  private void testControlChars(boolean remove, String input, String output) throws Exception {
    try (BufferedReader br = new BufferedReader(new OverrunReader( new StringReader(input), -1, false, remove))) {
      String line = br.readLine();
      Assert.assertEquals(output, line);
    }
  }

  @Test
  public void testReadKeepControlChars() throws Exception {
    testControlChars(false, "foo\0bar", "foo\0bar");
  }

  @Test
  public void testReadRemoveControlChars() throws Exception {
    testControlChars(true, "foo\0bar\0", "foobar");
  }

  private void testControlCharsBuffer(boolean remove, String input, String output) throws Exception {
    try (Reader reader = new OverrunReader( new StringReader(input), -1, false, remove)) {
      CharBuffer buff = CharBuffer.allocate(10);
      Assert.assertEquals(output.length(), reader.read(buff));
      buff.flip();
      Assert.assertEquals(output, buff.toString());
    }
  }

  @Test
  public void testReadKeepControlCharsBuffer() throws Exception {
    testControlCharsBuffer(false, "foo\0bar", "foo\0bar");
  }

  @Test
  public void testReadRemoveControlCharsBuffer() throws Exception {
    testControlCharsBuffer(true, "foo\0bar", "foobar");
  }

  @Test
  public void testMultipleCharsRemoval() throws Exception {
    testControlChars(true, "{\"a\":\0 \"foo\2ba\3r\4\"}", "{\"a\": \"foobar\"}");
  }

}
