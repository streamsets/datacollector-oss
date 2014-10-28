/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestBootstrapMain {

  private static final String[][] INVALID_ARGS = {
      {},
      {"a"},
      {"-mainClass"},
      {"-mainClass","a"},
      {"-mainClass", "a", "-apiClasspath"},
      {"-mainClass", "a", "-apiClasspath", "b"},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath"},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c"},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c", "-stageLibrariesDir"},
  };

  @Test
  public void testMissingOptions() throws Exception {
    for (String[] args : INVALID_ARGS) {
      try {
        BootstrapMain.main(args);
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        //NOP
      }
    }
  }

  // we are expecting a RuntimeException because the passed the options checking
  @Test(expected = RuntimeException.class)
  public void testAllOptions() throws Exception {
      BootstrapMain.main(new String[] {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c",
          "-stageLibrariesDir", "d"});
  }

  private String extractPathFromUrlString(String url) {
    Assert.assertTrue(url.startsWith("file:"));
    String path = url.substring("file:".length());
    if (path.startsWith("///")) {
      path = path.substring(2);
    }
    return path;
  }

  private String getBaseDir() {
    URL dummyResource = getClass().getClassLoader().getResource("dummy-resource.properties");
    Assert.assertNotNull(dummyResource);
    String path = dummyResource.toExternalForm();
    path = extractPathFromUrlString(path);
    return new File(path).getAbsoluteFile().getParent();
  }

  @Test
  public void testGetClasspathUrls1() throws Exception {
    List<URL> urls = BootstrapMain.getClasspathUrls("");
    Assert.assertTrue(urls.isEmpty());
  }

  @Test
  public void testGetClasspathUrlsDir1() throws Exception {
    String baseDir = getBaseDir();
    String classpath = baseDir + BootstrapMain.FILE_SEPARATOR + "conf-dir";

    List<URL> urls = BootstrapMain.getClasspathUrls(classpath);
    Assert.assertEquals(1, urls.size());
    Assert.assertTrue(urls.get(0).toExternalForm().endsWith("/"));
  }

  @Test
  public void testGetClasspathUrlsDir2() throws Exception {
    String baseDir = getBaseDir();
    String classpath = baseDir + BootstrapMain.FILE_SEPARATOR + "conf-dir" + BootstrapMain.FILE_SEPARATOR;

    List<URL> urls = BootstrapMain.getClasspathUrls(classpath);
    Assert.assertEquals(1, urls.size());
    Assert.assertTrue(urls.get(0).toExternalForm().endsWith("/"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClasspathUrlsNonExistentDir() throws Exception {
    String baseDir = getBaseDir();
    String classpath = baseDir + BootstrapMain.FILE_SEPARATOR + "invalid-dir";

    BootstrapMain.getClasspathUrls(classpath);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClasspathUrlsExpectedDirIsNoDir() throws Exception {
    String baseDir = getBaseDir();
    String classpath = baseDir + BootstrapMain.FILE_SEPARATOR + "dummy-resource.properties";

    BootstrapMain.getClasspathUrls(classpath);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClasspathUrlsJarsDirDoesNotExist() throws Exception {
    String baseDir = getBaseDir();
    String classpath = baseDir + BootstrapMain.FILE_SEPARATOR + "invalid-dir" + BootstrapMain.FILE_SEPARATOR + "*.jar";

    BootstrapMain.getClasspathUrls(classpath);
  }

  @Test
  public void testGetClasspathUrlsJars() throws Exception {
    String baseDir = getBaseDir();
    String jarsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "jars-dir" + BootstrapMain.FILE_SEPARATOR;
    String classpath = jarsDir + "*.jar";
    List<URL> urls = BootstrapMain.getClasspathUrls(classpath);

    Assert.assertEquals(2, urls.size());
    Set<String> got = new HashSet<String>();
    for (URL url : urls) {
      got.add(extractPathFromUrlString(url.toExternalForm()));
    }
    Assert.assertEquals(ImmutableSet.of(jarsDir + "dummy1.jar", jarsDir + "dummy2.jar"), got);
  }

  @Test
  public void testGetClasspathUrlsJarsAndDir() throws Exception {
    String baseDir = getBaseDir();
    String jarsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "jars-dir" + BootstrapMain.FILE_SEPARATOR;
    String classpath = jarsDir + "*.jar" + BootstrapMain.CLASSPATH_SEPARATOR +
                       baseDir + BootstrapMain.FILE_SEPARATOR + "conf-dir";
    List<URL> urls = BootstrapMain.getClasspathUrls(classpath);

    Assert.assertEquals(3, urls.size());
    Set<String> got = new HashSet<String>();
    for (URL url : urls) {
      got.add(extractPathFromUrlString(url.toExternalForm()));
    }
    Assert.assertEquals(
        ImmutableSet.of(jarsDir + "dummy1.jar", jarsDir + "dummy2.jar",
                        baseDir + BootstrapMain.FILE_SEPARATOR + "conf-dir" + BootstrapMain.FILE_SEPARATOR), got);
  }

  @Test
  public void testGetStageLibrariesClasspaths() throws Exception {
    String baseDir = getBaseDir();
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "stage-libs";
    Map<String, List<URL>> libs = BootstrapMain.getStageLibrariesClasspaths(stageLibsDir);
    Assert.assertEquals(2, libs.size());
    Assert.assertEquals(3, libs.get("stage1").size());
    Assert.assertEquals(2, libs.get("stage2").size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStageLibrariesClasspathsInvalidLibs() throws Exception {
    String baseDir = getBaseDir();
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "invalid-libs";
    BootstrapMain.getStageLibrariesClasspaths(stageLibsDir);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStageLibrariesClasspathsInvalidStageLib() throws Exception {
    String baseDir = getBaseDir();
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "stage-libs-invalid-lib";
    BootstrapMain.getStageLibrariesClasspaths(stageLibsDir);
  }

  private static boolean setClassLoaders;
  private static boolean main;

  public static class TMain {
    public static void setClassLoaders(ClassLoader api, ClassLoader container, List<ClassLoader> libs) {
      Assert.assertNotNull(api);
      Assert.assertNotNull(container);
      Assert.assertEquals(2, libs.size());
      setClassLoaders = true;
    }

    public static void main()  {
      main = true;
    }
  }

  @Test
  public void testMainInvocation() throws Exception {
    String baseDir = getBaseDir();
    String apiDir = baseDir + BootstrapMain.FILE_SEPARATOR + "jars-dir";
    String confDir = baseDir + BootstrapMain.FILE_SEPARATOR + "conf-dir";
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "stage-libs";

    setClassLoaders = false;
    main = false;
    BootstrapMain.main(new String[] {"-mainClass", TMain.class.getName(), "-apiClasspath", apiDir,
        "-containerClasspath", confDir, "-stageLibrariesDir", stageLibsDir});
    Assert.assertTrue(setClassLoaders);
    Assert.assertTrue(main);
  }

  @Test
  public void testMainInvocationWithDebug() throws Exception {
    System.setProperty("pipeline.bootstrap.debug", "true");
    try {
      testMainInvocation();
    } finally {
      System.getProperties().remove("pipeline.bootstrap.debug");
    }
  }

  @Test
  public void testConstructor() {
    new BootstrapMain();
  }

}
