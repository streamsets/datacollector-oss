/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class TestLongTypeSupport {

  @Test
  public void testCreate() {
    LongTypeSupport ts = new LongTypeSupport();
    long o = 1;
    Assert.assertSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    LongTypeSupport ts = new LongTypeSupport();
    long o = 1;
    Assert.assertSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    LongTypeSupport ts = new LongTypeSupport();
    long o = 1;
    Assert.assertSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() {
    LongTypeSupport support = new LongTypeSupport();
    Assert.assertEquals(new Long(1), support.convert("1"));
    Assert.assertEquals(new Long(1), support.convert((byte)1));
    Assert.assertEquals(new Long(1), support.convert((short)1));
    Assert.assertEquals(new Long(1), support.convert((int)1));
    Assert.assertEquals(new Long(1), support.convert((long)1));
    Assert.assertEquals(new Long(1), support.convert((float)1));
    Assert.assertEquals(new Long(1), support.convert((double)1));
    Assert.assertEquals(new Long(1), support.convert(new BigDecimal(1)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    LongTypeSupport support = new LongTypeSupport();
    new LongTypeSupport().convert(new Exception());
  }

}
