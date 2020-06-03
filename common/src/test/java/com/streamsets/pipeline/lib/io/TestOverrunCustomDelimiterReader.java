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

import org.junit.Assert;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;

public class TestOverrunCustomDelimiterReader {

  @Test
  public void testNormalNewLineWithoutDelimiters() throws Exception {
    Reader reader = new StringReader("1234567890\r\n123\r\n1\r\n");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, "\r\n", false);
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("1234567890", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("123", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("1", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testXmlElementsWithDelimiter() throws Exception {
    String record1 = "<note>\n" +
        "<to>a</to>\n" +
        "<from>b</from>\n" +
        "<heading>Record</heading>\n" +
        "<body>Record1</body>\n" +
        "</note>";
    String record2 = "<note>\n" +
        "<to>c</to>\n" +
        "<from>d</from>\n" +
        "<heading>Record</heading>\n" +
        "<body>Record2</body>\n" +
        "</note>";
    Reader reader = new StringReader(record1 + "\n" + record2 + "\n");

    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, record1.length() + 20, "</note>\n", true);
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals(record1 + "\n", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals(record2 + "\n", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testDelimiterPartialPresence() throws Exception {
    Reader reader = new StringReader("1234567890ABABABABC123ABC");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, "ABC", false);
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("1234567890ABABAB", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("123", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testWithSmallBuffer() throws Exception {
    Reader reader = new StringReader("1234567890ABABABABC123ABC");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, 10, "ABC", false);
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("1234567890ABABAB", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("123", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testDelimiterAcrossBufferSize() throws Exception {
    Reader reader = new StringReader("12345678ABC123ABC");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, 10, "ABC", false);
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("12345678", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("123", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testDelimiterAcrossBufferSizeWithDelimiter() throws Exception {
    Reader reader = new StringReader("12345678ABC123ABC");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, 10, "ABC", true);
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("12345678ABC", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("123ABC", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testQuotedDelimiter() throws Exception {
    Reader reader = new StringReader("\"1234xxx567\"89xxxABC");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, "xxx", false, '"', '\\');
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("\"1234xxx567\"89", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("ABC", sb.toString());

    delimiterReader.close();
  }

  @Test
  public void testQuotedDelimiterEscapeQuote() throws Exception {
    Reader reader = new StringReader("\"123\\\"4xxx567\"89xxxABC");
    OverrunCustomDelimiterReader delimiterReader = new OverrunCustomDelimiterReader(reader, 30, "xxx", false, '"', '\\');
    StringBuilder sb = new StringBuilder();

    delimiterReader.readLine(sb);
    Assert.assertEquals("\"123\\\"4xxx567\"89", sb.toString());

    sb.setLength(0);
    delimiterReader.readLine(sb);
    Assert.assertEquals("ABC", sb.toString());

    delimiterReader.close();
  }

}
