/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import org.junit.Assert;
import org.junit.Test;

public class TestModuleInfo {

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor1() {
    new ModuleInfo(null, null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor2() {
    new ModuleInfo("n", null, null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor3() {
    new ModuleInfo("n", "v", null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor4() {
    new ModuleInfo("n", "v", "d", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyName() {
    new ModuleInfo("", "v", "d", "n");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyInstanceName() {
    new ModuleInfo("n", "v", "d", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidName() {
    new ModuleInfo("pipeline", "v", "d", "n");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidInstanceName() {
    new ModuleInfo("n", "v", "d", "pipeline");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharInName() {
    new ModuleInfo("n:n", "v", "d", "n");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCharInInstanceName() {
    new ModuleInfo("n", "v", "d", "n:n");
  }

  @Test
  public void testModuleInfo() {
    ModuleInfo info = new ModuleInfo("n", "v", "d", "in");
    Assert.assertEquals("n", info.getName());
    Assert.assertEquals("v", info.getVersion());
    Assert.assertEquals("d", info.getDescription());
    Assert.assertEquals("in", info.getInstanceName());
  }
}
