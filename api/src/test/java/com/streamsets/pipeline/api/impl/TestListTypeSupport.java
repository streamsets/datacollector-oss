/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.Field;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestListTypeSupport {

  @Test
  public void testCreate() {
    ListTypeSupport ts = new ListTypeSupport();
    List map = new ArrayList();
    Assert.assertEquals(map, ts.create(map));
    Assert.assertSame(map, ts.create(map));
  }

  @Test
  public void testGet() {
    ListTypeSupport ts = new ListTypeSupport();
    List map = new ArrayList();
    Assert.assertEquals(map, ts.get(map));
    Assert.assertSame(map, ts.get(map));
  }

  @Test
  public void testClone() {
    ListTypeSupport ts = new ListTypeSupport();
    List map = new ArrayList();
    Assert.assertEquals(map, ts.clone(map));
    Assert.assertNotSame(map, ts.clone(map));
  }


  @Test
  public void testConvertValid() throws Exception {
    ListTypeSupport support = new ListTypeSupport();
    List<Field> m = new ArrayList<>();
    Assert.assertEquals(m, support.convert(m));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new ListTypeSupport().convert(new Exception());
  }

}
