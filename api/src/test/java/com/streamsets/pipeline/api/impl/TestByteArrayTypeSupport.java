/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.impl;

import org.junit.Assert;
import org.junit.Test;

public class TestByteArrayTypeSupport {

  @Test
  public void testCreate() {
    ByteArrayTypeSupport ts = new ByteArrayTypeSupport();
    byte[] array = new byte[0];
    Assert.assertArrayEquals(array, (byte[])ts.create(array));
    Assert.assertNotSame(array, ts.create(array));
  }

  @Test
  public void testGet() {
    ByteArrayTypeSupport ts = new ByteArrayTypeSupport();
    byte[] array = new byte[0];
    Assert.assertArrayEquals(array, (byte[])ts.get(array));
    Assert.assertNotSame(array, ts.get(array));
  }

  @Test
  public void testClone() {
    ByteArrayTypeSupport ts = new ByteArrayTypeSupport();
    byte[] array = new byte[0];
    Assert.assertArrayEquals(array, (byte[])ts.clone(array));
    Assert.assertNotSame(array, ts.clone(array));
  }

  @Test
  public void testConvertValid() throws Exception {
    ByteArrayTypeSupport support = new ByteArrayTypeSupport();
    byte[] array = new byte[0];
    Assert.assertArrayEquals(array, support.convert(array));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertInValid() {
    new ByteArrayTypeSupport().convert(new Exception());
  }

}
