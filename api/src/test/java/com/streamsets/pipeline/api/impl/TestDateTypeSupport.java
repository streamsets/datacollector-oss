/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class TestDateTypeSupport {

  @Test
  public void testCreate() {
    DateTypeSupport ts = new DateTypeSupport();
    Date o = new Date();
    Assert.assertEquals(o, ts.create(o));
    Assert.assertNotSame(o, ts.create(o));
  }

  @Test
  public void testGet() {
    DateTypeSupport ts = new DateTypeSupport();
    Date o = new Date();
    Assert.assertEquals(o, ts.get(o));
    Assert.assertNotSame(o, ts.get(o));
  }

  @Test
  public void testClone() {
    DateTypeSupport ts = new DateTypeSupport();
    Date o = new Date();
    Assert.assertEquals(o, ts.clone(o));
    Assert.assertNotSame(o, ts.clone(o));
  }

  @Test
  public void testConvertValid() throws Exception {
    DateTypeSupport support = new DateTypeSupport();
    Date d = new Date();
    Assert.assertEquals(d, support.convert(d));
    d = Utils.parse("2014-10-22T13:30Z");
    Assert.assertEquals(d, support.convert("2014-10-22T13:30Z"));
    Assert.assertEquals(d, support.convert(d.getTime()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid1() {
    new DateTypeSupport().convert(new Exception());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid2() {
    new DateTypeSupport().convert("2014");
  }

}
