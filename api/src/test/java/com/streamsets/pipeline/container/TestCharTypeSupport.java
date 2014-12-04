/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Test;

public class TestCharTypeSupport {

  @Test
  public void testCreate() {
    CharTypeSupport ts = new CharTypeSupport();
    char o = 'c';
    Assert.assertSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    CharTypeSupport ts = new CharTypeSupport();
    char o = 'c';
    Assert.assertSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    CharTypeSupport ts = new CharTypeSupport();
    char o = 'c';
    Assert.assertSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() {
    CharTypeSupport support = new CharTypeSupport();
    Assert.assertEquals(new Character('c'), support.convert('c'));
    Assert.assertEquals(new Character('c'), support.convert("c"));
    Assert.assertEquals(new Character('c'), support.convert("cX"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid1() {
    new CharTypeSupport().convert(new Exception());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid2() {
    new CharTypeSupport().convert("");
  }

}
