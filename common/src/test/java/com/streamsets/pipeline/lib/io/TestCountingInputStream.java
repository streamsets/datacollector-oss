/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TestCountingInputStream {

  private static final String DATA = "0123456789";

  @Test
  public void test() throws IOException {
    CountingInputStream countingInputStream = new CountingInputStream(new ByteArrayInputStream(DATA.getBytes()));
    Assert.assertEquals(0, countingInputStream.getCount());
    Assert.assertNotEquals(-1, countingInputStream.read());
    Assert.assertEquals(1, countingInputStream.getCount());
    Assert.assertEquals(2, countingInputStream.read(new byte[2]));
    Assert.assertEquals(3, countingInputStream.getCount());
    Assert.assertEquals(1, countingInputStream.skip(1));
    Assert.assertEquals(4, countingInputStream.getCount());
    countingInputStream.mark(2);
    Assert.assertEquals(4, countingInputStream.getCount());
    Assert.assertNotEquals(-1, countingInputStream.read());
    Assert.assertEquals(5, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(4, countingInputStream.getCount());
    countingInputStream.mark(1);
    Assert.assertEquals(2, countingInputStream.read(new byte[2]));
    Assert.assertEquals(6, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(6, countingInputStream.getCount());
    countingInputStream.mark(1);
    countingInputStream.resetCount();
    Assert.assertEquals(0, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(0, countingInputStream.getCount());
    Assert.assertNotEquals(-1, countingInputStream.read());
    Assert.assertEquals(1, countingInputStream.getCount());
    countingInputStream.mark(1);
    Assert.assertNotEquals(-1, countingInputStream.read());
    countingInputStream.resetCount();
    Assert.assertEquals(0, countingInputStream.getCount());
    countingInputStream.reset();
    Assert.assertEquals(0, countingInputStream.getCount());
  }

}
