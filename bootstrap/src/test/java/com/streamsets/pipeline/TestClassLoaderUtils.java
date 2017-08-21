/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
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
    Assert.assertArrayEquals(new String[]{"a", "b"}, ClassLoaderUtil.getTrimmedStrings("\na\n ,\n b"));
    Properties properties = new Properties();
    InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("api-classloader.properties");
    properties.load(is);
    is.close();
    String[] classes = ClassLoaderUtil.getTrimmedStrings(properties.getProperty("system.classes.default"));
    Assert.assertEquals(String.valueOf(properties), "java.", classes[0]);
    Assert.assertEquals(String.valueOf(properties), "-java.sql.Driver", classes[1]);
    Assert.assertEquals(String.valueOf(properties), "jdk.", classes[2]);
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
  @SuppressWarnings("unchecked")
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
