/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

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


}
