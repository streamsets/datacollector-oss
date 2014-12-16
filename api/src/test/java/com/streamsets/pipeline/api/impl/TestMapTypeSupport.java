/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestMapTypeSupport {

  @Test
  public void testCreate() {
    MapTypeSupport ts = new MapTypeSupport();
    Map map = new HashMap();
    Assert.assertEquals(map, ts.create(map));
    Assert.assertSame(map, ts.create(map));
  }

  @Test
  public void testGet() {
    MapTypeSupport ts = new MapTypeSupport();
    Map map = new HashMap();
    Assert.assertEquals(map, ts.get(map));
    Assert.assertSame(map, ts.get(map));
  }

  @Test
  public void testClone() {
    MapTypeSupport ts = new MapTypeSupport();
    Map map = new HashMap();
    Assert.assertEquals(map, ts.clone(map));
    Assert.assertNotSame(map, ts.clone(map));
  }

  @Test
  public void testConvertValid() throws Exception {
    MapTypeSupport support = new MapTypeSupport();
    Map m = new HashMap<>();
    Assert.assertEquals(m, support.convert(m));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new MapTypeSupport().convert(new Exception());
  }

}
