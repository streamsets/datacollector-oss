/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Test;

public class TestTypeSupport {

  public static class TTypeSupport extends TypeSupport<Object> {
    @Override
    public Object convert(Object value) {
      return value;
    }
  }

  @Test
  public void testCreate() {
    TypeSupport tt = new TTypeSupport();
    Object o = new Object();
    Assert.assertSame(o, tt.create(o));
  }

  @Test
  public void testGet() {
    TypeSupport tt = new TTypeSupport();
    Object o = new Object();
    Assert.assertSame(o, tt.get(o));
  }

  @Test
  public void testClone() {
    TypeSupport tt = new TTypeSupport();
    Object o = new Object();
    Assert.assertSame(o, tt.clone(o));
  }

}
