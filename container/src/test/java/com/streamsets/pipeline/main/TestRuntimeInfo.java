/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.main;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

public class TestRuntimeInfo {

  @Before
  @After
  public void cleanUp() {
    System.getProperties().remove("pipeline.conf.dir");
    System.getProperties().remove("pipeline.log.dir");
    System.getProperties().remove("pipeline.data.dir");
    System.getProperties().remove("pipeline.static-web.dir");
  }

  @Test
  public void testInfoDefault() {
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    Assert.assertEquals(System.getProperty("user.dir"), info.getRuntimeDir());
    Assert.assertEquals(System.getProperty("user.dir") + "/static-web", info.getStaticWebDir());
    Assert.assertEquals(System.getProperty("user.dir") + "/etc", info.getConfigDir());
    Assert.assertEquals(System.getProperty("user.dir") + "/log", info.getLogDir());
    Assert.assertEquals(System.getProperty("user.dir") + "/var", info.getDataDir());
    Assert.assertEquals(Arrays.asList(getClass().getClassLoader()), info.getStageLibraryClassLoaders());
    Logger log = Mockito.mock(Logger.class);
    info.log(log);
  }

  @Test
  public void testInfoCustom() {
    System.setProperty("pipeline.static-web.dir", "w");
    System.setProperty("pipeline.conf.dir", "x");
    System.setProperty("pipeline.log.dir", "y");
    System.setProperty("pipeline.data.dir", "z");

    List<? extends ClassLoader> customCLs = Arrays.asList(new URLClassLoader(new URL[0], null));
    RuntimeInfo info = new RuntimeInfo(customCLs);
    Assert.assertEquals(System.getProperty("user.dir"), info.getRuntimeDir());
    Assert.assertEquals("w", info.getStaticWebDir());
    Assert.assertEquals("x", info.getConfigDir());
    Assert.assertEquals("y", info.getLogDir());
    Assert.assertEquals("z", info.getDataDir());
    Assert.assertEquals(customCLs, info.getStageLibraryClassLoaders());
    Logger log = Mockito.mock(Logger.class);
    info.log(log);
  }

  @Test
  public void testAttributes() {
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    Assert.assertFalse(info.hasAttribute("a"));
    info.setAttribute("a", 1);
    Assert.assertTrue(info.hasAttribute("a"));
    Assert.assertEquals(1, info.getAttribute("a"));
    info.removeAttribute("a");
    Assert.assertFalse(info.hasAttribute("a"));
  }

}
