/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

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
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c", "-streamsetsLibrariesDir"},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c", "-streamsetsLibrariesDir", "d"},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c", "-streamsetsLibrariesDir", "d",
          "-userLibrariesDir"},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c", "-streamsetsLibrariesDir", "d",
          "-userLibrariesDir", "e",},
      {"-mainClass", "a", "-apiClasspath", "b", "-containerClasspath", "c", "-streamsetsLibrariesDir", "d",
          "-userLibrariesDir", "e", "-configDir"},
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
          "-streamsetsLibrariesDir", "d", "-userLibrariesDir", "e", "-configDir", "f"});
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
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "streamsets-libs";
    Map<String, List<URL>> libs = BootstrapMain.getStageLibrariesClasspaths(stageLibsDir, null);
    Assert.assertEquals(String.valueOf(libs), 2, libs.size());
    Assert.assertNotNull(String.valueOf(libs.keySet()), libs.get("streamsets-libs/stage1"));
    Assert.assertNotNull(String.valueOf(libs.keySet()), libs.get("streamsets-libs/stage2"));
    Assert.assertEquals(String.valueOf(libs.get("streamsets-libs/stage1")), 3,
      libs.get("streamsets-libs/stage1").size());
    Assert.assertEquals(String.valueOf(libs.get("streamsets-libs/stage2")), 2,
      libs.get("streamsets-libs/stage2").size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStageLibrariesClasspathsInvalidLibs() throws Exception {
    String baseDir = getBaseDir();
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "invalid-libs";
    BootstrapMain.getStageLibrariesClasspaths(stageLibsDir, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStageLibrariesClasspathsInvalidStageLib() throws Exception {
    String baseDir = getBaseDir();
    String stageLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "stage-libs-invalid-lib";
    BootstrapMain.getStageLibrariesClasspaths(stageLibsDir, null);
  }

  private static boolean setClassLoaders;
  private static boolean main;

  public static class TMain {
    public static void setContext(ClassLoader api, ClassLoader container,
                                  List<? extends ClassLoader> libs, Instrumentation instrumentation) {
      Assert.assertNotNull(api);
      Assert.assertNotNull(container);
      Assert.assertEquals(3, libs.size());
      setClassLoaders = true;
    }

    public static void main(String[] args)  {
      main = true;
    }
  }

  public static class TMainWhiteList {
    public static void setContext(ClassLoader api, ClassLoader container,
        List<? extends ClassLoader> libs, Instrumentation instrumentation) {
      Assert.assertNotNull(api);
      Assert.assertNotNull(container);
      Assert.assertEquals(1, libs.size());
      Assert.assertEquals("stage1", ((SDCClassLoader) libs.get(0)).getName());
      setClassLoaders = true;
    }

    public static void main(String[] args)  {
      main = true;
    }
  }

  @Test
  public void testMainInvocation() throws Exception {
    String baseDir = getBaseDir();
    String apiDir = baseDir + BootstrapMain.FILE_SEPARATOR + "jars-dir";
    String confDir = baseDir + BootstrapMain.FILE_SEPARATOR + "conf-dir";
    String streamsetsLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "streamsets-libs";
    String userLibsDir = baseDir + BootstrapMain.FILE_SEPARATOR + "user-libs";

    File dir = new File(confDir);
    dir.mkdirs();
    Properties props = new Properties();
    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, "*");
    props.setProperty(BootstrapMain.USER_LIBS_KEY, "*");
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    setClassLoaders = false;
    main = false;
    BootstrapMain.main(new String[]{"-mainClass", TMain.class.getName(), "-apiClasspath", apiDir,
        "-containerClasspath", confDir, "-streamsetsLibrariesDir", streamsetsLibsDir, "-userLibrariesDir",
        userLibsDir, "-configDir", confDir});
    Assert.assertTrue(setClassLoaders);
    Assert.assertTrue(main);

    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, "stage1");
    props.setProperty(BootstrapMain.USER_LIBS_KEY, "");
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    BootstrapMain.main(new String[]{"-mainClass", TMainWhiteList.class.getName(), "-apiClasspath", apiDir,
        "-containerClasspath", confDir, "-streamsetsLibrariesDir", streamsetsLibsDir, "-userLibrariesDir",
        userLibsDir, "-configDir", confDir});

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

  @Test(expected = RuntimeException.class)
  public void testGetWhiteListMissingDir() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    BootstrapMain.getWhiteList(dir.getAbsolutePath(), null);
  }

  @Test(expected = RuntimeException.class)
  public void testGetWhiteListMissingFile() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    BootstrapMain.getWhiteList(dir.getAbsolutePath(), null);
  }

  @Test(expected = RuntimeException.class)
  public void testGetWhiteListMissingProperty() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    Properties props = new Properties();
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    BootstrapMain.getWhiteList(dir.getAbsolutePath(), BootstrapMain.SYSTEM_LIBS_KEY);
  }

  @Test
  public void testGetWhiteListAllValues() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    Properties props = new Properties();
    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, BootstrapMain.ALL_VALUES);
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    Assert.assertNull(BootstrapMain.getWhiteList(dir.getAbsolutePath(), BootstrapMain.SYSTEM_LIBS_KEY));
  }

  @Test
  public void testGetWhiteListNoValues() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    Properties props = new Properties();
    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, "");
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    Assert.assertTrue(BootstrapMain.getWhiteList(dir.getAbsolutePath(), BootstrapMain.SYSTEM_LIBS_KEY).isEmpty());

    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, " ");
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    Assert.assertTrue(BootstrapMain.getWhiteList(dir.getAbsolutePath(), BootstrapMain.SYSTEM_LIBS_KEY).isEmpty());

    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, ",,");
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    Assert.assertTrue(BootstrapMain.getWhiteList(dir.getAbsolutePath(), BootstrapMain.SYSTEM_LIBS_KEY).isEmpty());
  }

  @Test
  public void testGetWhiteListCustomValues() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dir.mkdirs());
    Properties props = new Properties();
    props.setProperty(BootstrapMain.SYSTEM_LIBS_KEY, "a, b ,");
    try (OutputStream os = new FileOutputStream(new File(dir, BootstrapMain.WHITE_LIST_FILE))) {
      props.store(os, "");
    }
    Assert.assertEquals(ImmutableSet.of("a","b"), BootstrapMain.getWhiteList(dir.getAbsolutePath(),
                                                                             BootstrapMain.SYSTEM_LIBS_KEY));
  }

}
