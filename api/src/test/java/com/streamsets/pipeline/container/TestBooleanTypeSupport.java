/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class TestBooleanTypeSupport {

  @Test
  public void testCreate() {
    BooleanTypeSupport ts = new BooleanTypeSupport();
    Boolean o = true;
    Assert.assertSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    BooleanTypeSupport ts = new BooleanTypeSupport();
    Boolean o = true;
    Assert.assertSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    BooleanTypeSupport ts = new BooleanTypeSupport();
    Boolean o = true;
    Assert.assertSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() {
    BooleanTypeSupport support = new BooleanTypeSupport();
    Assert.assertEquals(true, support.convert(true));
    Assert.assertEquals(true, support.convert("true"));
    Assert.assertEquals(true, support.convert(1));
    Assert.assertEquals(true, support.convert((long) 1));
    Assert.assertEquals(true, support.convert((short) 1));
    Assert.assertEquals(true, support.convert((byte) 1));
    Assert.assertEquals(true, support.convert((float) 1));
    Assert.assertEquals(true, support.convert((double) 1));
    Assert.assertEquals(true, support.convert(new BigDecimal(1)));

    Assert.assertEquals(false, support.convert(false));
    Assert.assertEquals(false, support.convert("false"));
    Assert.assertEquals(false, support.convert(0));
    Assert.assertEquals(false, support.convert((long) 0));
    Assert.assertEquals(false, support.convert((short) 0));
    Assert.assertEquals(false, support.convert((byte) 0));
    Assert.assertEquals(false, support.convert((float) 0));
    Assert.assertEquals(false, support.convert((double) 0));
    Assert.assertEquals(false, support.convert(new BigDecimal(0)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new BooleanTypeSupport().convert(new Exception());
  }

}
