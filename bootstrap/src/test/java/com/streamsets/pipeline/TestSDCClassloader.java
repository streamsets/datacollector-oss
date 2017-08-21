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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.TreeSet;

public class TestSDCClassloader {

  @Test
  @SuppressWarnings("unchecked")
  public void testServices() throws Exception {
    SystemPackage systemPackage = new SystemPackage(Arrays.asList("org.apache.hadoop.fs."));
    Assert.assertTrue(systemPackage.isSystem(ClassLoaderUtil.SERVICES_PREFIX +
      "org.apache.hadoop.fs.FileSystem"));
  }

  private static class CallStoringURLClassLoader extends SDCClassLoader {
    final List<String> calls = new ArrayList<>();

    public CallStoringURLClassLoader(ClassLoader parent, String systemClasses, String appClasses) {
      super("test", "somecl", Arrays.<URL>asList(), parent,
        new String[0], new SystemPackage(ClassLoaderUtil.getTrimmedStrings(systemClasses)),
        new ApplicationPackage(new TreeSet<String>(Arrays.asList(ClassLoaderUtil.getTrimmedStrings(appClasses)))),
          false, false, false);
    }

    @Override
    public URL findResource(String name) {
      calls.add("findResource " + name);
      return super.findResource(name);
    }

    @Override
    public Enumeration<URL> findResources(String name) throws IOException {
      calls.add("findResources " + name);
      return super.findResources(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
      calls.add("loadClass " + name);
      return super.loadClass(name, resolve);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testClassLoaderOrder() throws Exception {
    CallStoringURLClassLoader parent = new CallStoringURLClassLoader(new URLClassLoader(new URL[0]), "", "");
    CallStoringURLClassLoader stage = new CallStoringURLClassLoader(parent, "sys.", "app.");
    // sys should be delegated to the parent
    doLoadClass(Arrays.asList("loadClass sys.Dummy"), stage, "sys.Dummy");
    // other should be delegated to the parent
    doLoadClass(Arrays.asList("loadClass other.Dummy"), stage, "other.Dummy");
    // app should be delegated to the parent
    doLoadClass(Arrays.<String>asList(), stage, "app.Dummy");

    doGetResource(Arrays.<String>asList("findResource /META-INF/services/sys.Dummy",
      "findResource META-INF/services/sys.Dummy"), stage, ClassLoaderUtil.SERVICES_PREFIX + "sys.Dummy");
    doGetResource(Arrays.<String>asList("findResource /META-INF/services/other.Dummy",
      "findResource META-INF/services/other.Dummy"), stage, ClassLoaderUtil.SERVICES_PREFIX + "other.Dummy");
    doGetResource(Arrays.<String>asList(), stage, ClassLoaderUtil.SERVICES_PREFIX + "app.Dummy");

    doGetResources(Arrays.<String>asList("findResources /META-INF/services/sys.Dummy"),
      stage, ClassLoaderUtil.SERVICES_PREFIX + "sys.Dummy");
    doGetResources(Arrays.<String>asList("findResources /META-INF/services/other.Dummy"),
      stage, ClassLoaderUtil.SERVICES_PREFIX + "other.Dummy");
    doGetResources(Arrays.<String>asList(), stage, ClassLoaderUtil.SERVICES_PREFIX + "app.Dummy");

    doGetResourceAsStream(Arrays.<String>asList("findResource /META-INF/services/sys.Dummy"), stage,
      ClassLoaderUtil.SERVICES_PREFIX + "sys.Dummy");
    doGetResourceAsStream(Arrays.<String>asList("findResource /META-INF/services/other.Dummy"), stage,
      ClassLoaderUtil.SERVICES_PREFIX + "other.Dummy");
    doGetResourceAsStream(Arrays.<String>asList(), stage, ClassLoaderUtil.SERVICES_PREFIX +
      "app.Dummy");

    doGetResource(Arrays.<String>asList("findResource sys.Dummy"), stage, "sys.Dummy");
    doGetResource(Arrays.<String>asList("findResource other.Dummy"), stage, "other.Dummy");
    doGetResource(Arrays.<String>asList(), stage, "app.Dummy");

    doGetResources(Arrays.<String>asList("findResources sys.Dummy"), stage, "sys.Dummy");
    doGetResources(Arrays.<String>asList("findResources other.Dummy"), stage, "other.Dummy");
    doGetResources(Arrays.<String>asList(), stage, "app.Dummy");

    doGetResourceAsStream(Arrays.<String>asList(
      "findResource sys.Dummy"), stage, "sys.Dummy");
    doGetResourceAsStream(Arrays.<String>asList(
      "findResource other.Dummy"), stage, "other.Dummy");
    doGetResourceAsStream(Arrays.<String>asList(), stage, "app.Dummy");
  }

  private static void doGetResourceAsStream(List<String> expectedCallsToParent,
                                            SDCClassLoader stage, String name) throws Exception {
    List<String> actualCallsToParent = ((CallStoringURLClassLoader) stage.getParent()).calls;
    Assert.assertNull(stage.getResourceAsStream(name));
    Assert.assertEquals(expectedCallsToParent, actualCallsToParent);
    actualCallsToParent.clear();
  }

  private static void doGetResources(List<String> expectedCallsToParent,
                                     SDCClassLoader stage, String name) throws Exception {
    List<String> actualCallsToParent = ((CallStoringURLClassLoader) stage.getParent()).calls;
    Enumeration<URL> enumeration = stage.getResources(name);
    Assert.assertFalse(enumeration.hasMoreElements());
    Assert.assertEquals(expectedCallsToParent, actualCallsToParent);
    actualCallsToParent.clear();
  }

  private static void doGetResource(List<String> expectedCallsToParent,
                                    SDCClassLoader stage, String name) {
    List<String> actualCallsToParent = ((CallStoringURLClassLoader) stage.getParent()).calls;
    Assert.assertNull(stage.getResource(name));
    Assert.assertEquals(expectedCallsToParent, actualCallsToParent);
    actualCallsToParent.clear();
  }

  private static void doLoadClass(List<String> expectedCallsToParent,
                                  SDCClassLoader stage, String name) {
    List<String> actualCallsToParent = ((CallStoringURLClassLoader) stage.getParent()).calls;
    try {
      stage.loadClass(name);
      Assert.fail("expected ClassNotFoundException");
    } catch (ClassNotFoundException ex) {
      // expected
    }
    Assert.assertEquals(expectedCallsToParent, actualCallsToParent);
    actualCallsToParent.clear();
  }

  // we are expecting a ClassFormatError because the class file is invalid
  @Test(expected = ClassFormatError.class)
  public void testDuplicateStageClassLoader() throws Exception {
    File dir = TestBlackListURLClassLoader.getBaseDir();
    SDCClassLoader cl = SDCClassLoader.getStageClassLoader("foo",
        "bar",
        Arrays.asList(dir.toURI().toURL(), new File(dir, "bar.jar").toURI().toURL()),
        getClass().getClassLoader()
    );
    Assert.assertFalse(cl.isPrivate());
    cl = cl.duplicateStageClassLoader();
    Assert.assertTrue(cl.isPrivate());
    cl.loadClass("x.y.Dummy");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBringStageLibAndProtoLibsToFront() throws Exception {

    List<URL> urls = ImmutableList.of(new URL("file:///tmp/bar-1.jar"));
    Assert.assertEquals(urls, SDCClassLoader.bringStageAndProtoLibsToFront("bar", urls));

    urls = ImmutableList.of(new URL("file:///tmp/foo-protolib-1.jar"), new URL("file:///tmp/bar-1.jar"));
    List<URL> expected = ImmutableList.of(new URL("file:///tmp/bar-1.jar"), new URL("file:///tmp/foo-protolib-1.jar"));
    Assert.assertEquals(expected, SDCClassLoader.bringStageAndProtoLibsToFront("bar", urls));

    urls = ImmutableList.of(new URL("file:///tmp/bar-1.jar"), new URL("file:///tmp/foo-protolib-1.jar"));
    Assert.assertEquals(urls, SDCClassLoader.bringStageAndProtoLibsToFront("bar", urls));

    urls = ImmutableList.of(
        new URL("file:///tmp/foo-1.jar"),
        new URL("file:///tmp/bar-1.jar"),
        new URL("file:///tmp/foo-protolib-1.jar")
    );
    expected = ImmutableList.of(
        new URL("file:///tmp/bar-1.jar"),
        new URL("file:///tmp/foo-protolib-1.jar"),
        new URL("file:///tmp/foo-1.jar")
    );
    Assert.assertEquals(expected, SDCClassLoader.bringStageAndProtoLibsToFront("bar", urls));

  }

  @Test(expected = ExceptionInInitializerError.class)
  public void testBringMultipleStageLibsToFront() throws Exception {
    List<URL> urls = ImmutableList.of(new URL("file:///tmp/bar-1.jar"), new URL("file:///tmp/bar-X-1.jar"));
    Assert.assertEquals(urls, SDCClassLoader.bringStageAndProtoLibsToFront("bar", urls));
  }
}
