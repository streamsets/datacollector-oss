/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Test;

public class TestStringTypeSupport {

  @Test
  public void testCreate() {
    StringTypeSupport ts = new StringTypeSupport();
    String o = "s";
    Assert.assertSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    StringTypeSupport ts = new StringTypeSupport();
    String o = "s";
    Assert.assertSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    StringTypeSupport ts = new StringTypeSupport();
    String o = "s";
    Assert.assertSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() {
    StringTypeSupport support = new StringTypeSupport();
    Assert.assertEquals("s", support.convert("s"));
  }

}
