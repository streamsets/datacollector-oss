/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class TestClassLoaderUtils {

  @Test
  public void testRealClassLoader() throws Exception {
    ApplicationPackage applicationPackage = new ApplicationPackage(Assert.class.getClassLoader());
    Assert.assertTrue(applicationPackage.isApplication("org.junit.SampleClass"));
    Assert.assertTrue(applicationPackage.isApplication("META-INF/services/org.junit.SampleService"));
    Assert.assertTrue(applicationPackage.isApplication("/META-INF/services/org.junit.SampleService"));
  }

  @Test
  public void testOptimalCase() throws Exception {
    SortedSet<String> packages = new TreeSet<>();
    packages.add("a.b.c.d.1.");
    packages.add("a.b.c.d.2.");
    packages.add("a.b.c.d.3.");
    packages.add("a.b.c.d.4.");
    packages.add("a.b.c.d.5.");
    packages.add("a.b.c.d.6.");
    ApplicationPackage applicationPackage;
    applicationPackage = new ApplicationPackage(packages);
    Assert.assertFalse(applicationPackage.isApplication("a.b.c.d.0.SimpleObjectFactoryFactory"));
    Assert.assertEquals(0, applicationPackage.getOperationCount());
    applicationPackage = new ApplicationPackage(packages);
    Assert.assertTrue(applicationPackage.isApplication("a.b.c.d.6.SimpleObjectFactoryFactory"));
    Assert.assertEquals(1, applicationPackage.getOperationCount());
    applicationPackage = new ApplicationPackage(packages);
    Assert.assertTrue(applicationPackage.isApplication("a.b.c.d.3.X"));
    Assert.assertEquals(1, applicationPackage.getOperationCount());
  }

  @Test
  public void testNormalCase() throws Exception {
    SortedSet<String> packages = new TreeSet<>();
    packages.add("a.b.c.d.e.1.");
    packages.add("a.b.c.d.e.2.x.y.z");
    packages.add("a.b.c.3.");
    packages.add("a.b.c.d.e.4.");
    packages.add("a.b.c.d.e.5.");
    packages.add("a.b.d.e.6.");
    ApplicationPackage applicationPackage;
    applicationPackage = new ApplicationPackage(packages);
    Assert.assertFalse(applicationPackage.isApplication("a.b.c.d.e.0.SimpleObjectFactoryFactory"));
    Assert.assertEquals(0, applicationPackage.getOperationCount());
    applicationPackage = new ApplicationPackage(packages);
    Assert.assertTrue(applicationPackage.isApplication("a.b.d.e.6.SimpleObjectFactoryFactory"));
    Assert.assertEquals(1, applicationPackage.getOperationCount());
    applicationPackage = new ApplicationPackage(packages);
    Assert.assertTrue(applicationPackage.isApplication("a.b.c.3.X"));
    Assert.assertEquals(1, applicationPackage.getOperationCount());
  }

  @Test
  public void testNotNull1() throws Exception {
    ClassLoaderUtil.checkNotNull("", "");
  }

  @Test(expected = NullPointerException.class)
  public void testNotNull2() throws Exception {
    ClassLoaderUtil.checkNotNull(null, "");
  }

  @Test
  public void testGetTrimmedStrings() throws Exception {
    Assert.assertArrayEquals(new String[]{"a", "b"}, ClassLoaderUtil.getTrimmedStrings("a , b"));
  }

  @Test
  public void testCanonicalize() throws Exception {
    Assert.assertEquals("a.b.c", ClassLoaderUtil.canonicalizeClass("///a/b/c"));
    Assert.assertEquals("a.b.c", ClassLoaderUtil.canonicalizeClassOrResource(ClassLoaderUtil.SERVICES_PREFIX + "a/b/c"));
  }

  @Test
  public void testSystemPackage() throws Exception {
    SystemPackage systemPackage = new SystemPackage(new String[] {"a.", "-a.b.", "b.Some"});
    Assert.assertTrue(systemPackage.isSystem("a.c"));
    Assert.assertFalse(systemPackage.isSystem("a.b.c"));
    Assert.assertTrue(systemPackage.isSystem("b.Some"));
    Assert.assertTrue(systemPackage.isSystem("b.Some$Inner"));
  }
  @Test
  public void testRemoveLogicalDuplicates() throws Exception {
    SortedSet<String> packages = new TreeSet<>();
    ApplicationPackage.removeLogicalDuplicates(packages);
    Assert.assertEquals(new TreeSet(), packages);
    packages.add("a.");
    packages.add("a.b.c");
    packages.add("a.d.e");
    packages.add("b.");
    packages.add("b.c.");
    packages.add("c.");
    ApplicationPackage.removeLogicalDuplicates(packages);
    Assert.assertEquals(new TreeSet(Arrays.asList("a.", "b.", "c.")), packages);
  }
}