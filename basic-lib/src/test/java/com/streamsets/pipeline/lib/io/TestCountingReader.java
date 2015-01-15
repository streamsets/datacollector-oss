/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class TestCountingReader {

  private static final String DATA = "0123456789";

  @Test
  public void test() throws IOException {
    CountingReader reader = new CountingReader(new StringReader(DATA));
    Assert.assertEquals(0, reader.getCount());
    Assert.assertNotEquals(-1, reader.read());
    Assert.assertEquals(1, reader.getCount());
    Assert.assertEquals(2, reader.read(new char[2]));
    Assert.assertEquals(3, reader.getCount());
    Assert.assertEquals(1, reader.skip(1));
    Assert.assertEquals(4, reader.getCount());
    reader.mark(2);
    Assert.assertEquals(4, reader.getCount());
    Assert.assertNotEquals(-1, reader.read());
    Assert.assertEquals(5, reader.getCount());
    reader.reset();
    Assert.assertEquals(4, reader.getCount());
    reader.mark(1);
    Assert.assertEquals(2, reader.read(new char[2]));
    Assert.assertEquals(6, reader.getCount());
    reader.reset();
    Assert.assertEquals(6, reader.getCount());
    reader.mark(1);
    reader.resetCount();
    Assert.assertEquals(0, reader.getCount());
    reader.reset();
    Assert.assertEquals(0, reader.getCount());
    Assert.assertNotEquals(-1, reader.read());
    Assert.assertEquals(1, reader.getCount());
    reader.mark(1);
    Assert.assertNotEquals(-1, reader.read());
    reader.resetCount();
    Assert.assertEquals(0, reader.getCount());
    reader.reset();
    Assert.assertEquals(0, reader.getCount());
  }

}
