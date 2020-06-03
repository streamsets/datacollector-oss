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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.api.ext.io.OverrunReader;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

public class TestOverrunLineReader {

  private Reader getReaderStream() {
    return new StringReader("1234567890\n\n\r123\r\n\n1");
  }

  @Test
  public void testReadLineSBUnderMax() throws Exception {
    OverrunLineReader lr = new OverrunLineReader(getReaderStream(), 1024);
    StringBuilder sb = new StringBuilder();
    Assert.assertEquals(10, lr.readLine(sb));
    Assert.assertEquals("1234567890", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("1234567890", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("1234567890", sb.toString());
    Assert.assertEquals(3, lr.readLine(sb));
    Assert.assertEquals("1234567890123", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("1234567890123", sb.toString());
    Assert.assertEquals(1, lr.readLine(sb));
    Assert.assertEquals("12345678901231", sb.toString());
    Assert.assertEquals(-1, lr.readLine(sb));
    Assert.assertEquals("12345678901231", sb.toString());
  }

  @Test
  public void testReadLineSBAtMax() throws Exception {
    OverrunLineReader lr = new OverrunLineReader(getReaderStream(), 10);
    StringBuilder sb = new StringBuilder();
    Assert.assertEquals(10, lr.readLine(sb));
    Assert.assertEquals("1234567890", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("1234567890", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("1234567890", sb.toString());
    Assert.assertEquals(3, lr.readLine(sb));
    Assert.assertEquals("1234567890123", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("1234567890123", sb.toString());
    Assert.assertEquals(1, lr.readLine(sb));
    Assert.assertEquals("12345678901231", sb.toString());
    Assert.assertEquals(-1, lr.readLine(sb));
    Assert.assertEquals("12345678901231", sb.toString());
  }

  @Test
  public void testReadLineSBOverMax() throws Exception {
    OverrunLineReader lr = new OverrunLineReader(getReaderStream(), 8);
    StringBuilder sb = new StringBuilder();
    Assert.assertEquals(10, lr.readLine(sb));
    Assert.assertEquals("12345678", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("12345678", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("12345678", sb.toString());
    Assert.assertEquals(3, lr.readLine(sb));
    Assert.assertEquals("12345678123", sb.toString());
    Assert.assertEquals(0, lr.readLine(sb));
    Assert.assertEquals("12345678123", sb.toString());
    Assert.assertEquals(1, lr.readLine(sb));
    Assert.assertEquals("123456781231", sb.toString());
    Assert.assertEquals(-1, lr.readLine(sb));
    Assert.assertEquals("123456781231", sb.toString());
  }

  @Test
  public void testPosNotAvail() throws Exception {
    OverrunLineReader lr = new OverrunLineReader(getReaderStream(), 8);
    Assert.assertEquals(-1, lr.getPos());
    Assert.assertEquals(10, lr.readLine(new StringBuilder()));
    Assert.assertEquals(-1, lr.getPos());
  }

  @Test
  public void testPos() throws Exception {
    OverrunLineReader lr = new OverrunLineReader(new OverrunReader(getReaderStream(), 100, false, false), 8);
    Assert.assertEquals(0, lr.getPos());
    Assert.assertEquals(10, lr.readLine(new StringBuilder()));
    Assert.assertEquals(11, lr.getPos());
    Assert.assertEquals(0, lr.readLine(new StringBuilder()));
    Assert.assertEquals(12, lr.getPos());
    Assert.assertEquals(0, lr.readLine(new StringBuilder()));
    Assert.assertEquals(13, lr.getPos());
    Assert.assertEquals(3, lr.readLine(new StringBuilder()));
    Assert.assertEquals(18, lr.getPos());
    Assert.assertEquals(0, lr.readLine(new StringBuilder()));
    Assert.assertEquals(19, lr.getPos());
    Assert.assertEquals(1, lr.readLine(new StringBuilder()));
    Assert.assertEquals(20, lr.getPos());
    Assert.assertEquals(-1, lr.readLine(new StringBuilder()));
    Assert.assertEquals(20, lr.getPos());
  }

  @Test
  public void testPosStartingAtOffset() throws Exception {
    OverrunLineReader lr = new OverrunLineReader(new OverrunReader(getReaderStream(), 100, false, false), 8);
    IOUtils.skipFully(lr, 18);
    Assert.assertEquals(18, lr.getPos());
    Assert.assertEquals(0, lr.readLine(new StringBuilder()));
    Assert.assertEquals(19, lr.getPos());
    Assert.assertEquals(1, lr.readLine(new StringBuilder()));
    Assert.assertEquals(20, lr.getPos());
    Assert.assertEquals(-1, lr.readLine(new StringBuilder()));
    Assert.assertEquals(20, lr.getPos());
  }

  @Test
  public void testQuotedDelimiter() throws Exception {
    Reader reader = new StringReader("\"1234\n567\"89\nABC");
    OverrunLineReader delimiterReader = new OverrunLineReader(new OverrunReader(reader, 100, false, false), 1024, '"', '\\');
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("\"1234\n567\"89", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("ABC", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testQuotedDelimiterEscapeQuote() throws Exception {
    Reader reader = new StringReader("\"123\\\"4\n567\"89\nABC");
    OverrunLineReader delimiterReader = new OverrunLineReader(new OverrunReader(reader, 100, false, false), 1024, '"', '\\');
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("\"123\\\"4\n567\"89", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("ABC", sb.toString());

    delimiterReader.close();
  }

}
