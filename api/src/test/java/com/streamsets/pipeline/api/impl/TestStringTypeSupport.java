/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

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

  @Test
  public void testConvertToString() {
    StringTypeSupport support = new StringTypeSupport();
    //String
    Assert.assertEquals("s", support.convert("s"));
    //Boolean
    Assert.assertEquals("false", support.convert(false));
    //Byte
    Assert.assertEquals("8", support.convert((byte) 8));
    //Character
    Assert.assertEquals("c", support.convert('c'));
    //Date
    Date date = new Date();
    Assert.assertEquals(date.toString(), support.convert(date));
    //Integer
    Assert.assertEquals("2543345", support.convert(2543345));
    //Long
    Assert.assertEquals("2543543782929292", support.convert(2543543782929292L));
    //Double
    Assert.assertEquals("254.896", support.convert(254.896));
    //Float
    Assert.assertEquals("254.896", support.convert(254.896F));
    //Decimal
    Assert.assertEquals("2335.4544999999998253770172595977783203125", support.convert(new BigDecimal(2335.4545)));

    try {
      support.convert(new HashMap<>());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      support.convert(new ArrayList<>());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }

    try {
      support.convert("Hello World".getBytes());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }
  }

}
