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
package com.streamsets.datacollector.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.datacollector.cluster.TarFileCreator;
import com.streamsets.pipeline.api.impl.Utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.GZIPInputStream;

public class TestTarFileCreator {
  private File tempDir;


  @Before
  public void setup() throws IOException {
    tempDir = Files.createTempDir();
  }

  @After
  public void tearDown() {
    if (tempDir != null) {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateEtcTarGzDirDoesNotExist() throws Exception {
    File etcDir = new File(tempDir, "etc");
    File tarFile = new File(tempDir, "etc.tar.gz");
    TarFileCreator.createTarGz(etcDir, tarFile);
  }

  @Test
  public void testCreateEtcTarGzDirIsEmpty() throws Exception {
    File etcDir = new File(tempDir, "etc");
    Assert.assertTrue(etcDir.mkdir());
    File tarFile = new File(tempDir, "etc.tar.gz");
    TarFileCreator.createTarGz(etcDir, tarFile);
    TarInputStream tis = new TarInputStream(new GZIPInputStream(new FileInputStream(tarFile)));
    Assert.assertNotNull(tis);
  }

  @Test
  public void testCreateEtcTarGz() throws Exception {
    File etcDir = new File(tempDir, "etc");
    Assert.assertTrue(etcDir.mkdir());
    createJar(etcDir);
    createJar(etcDir);
    File tarFile = new File(tempDir, "etc.tar.gz");
    TarFileCreator.createTarGz(etcDir, tarFile);
    TarInputStream tis = new TarInputStream(new GZIPInputStream(new FileInputStream(tarFile)));
    readJar(tis);
  }

  @Test
  public void testCreateLibsTarGz() throws Exception {
    File apiLibDir = new File(tempDir, "api-lib");
    File containerLibDir = new File(tempDir, "container-lib");
    File streamsetsLibsDir = new File(tempDir, "streamsets-libs");
    File userLibsDir = new File(tempDir, "user-libs");
    URLClassLoader apiCl = new URLClassLoader(new URL[]{createJar(apiLibDir).toURI().toURL()});
    URLClassLoader containerCL = new URLClassLoader(new URL[]{createJar(containerLibDir).toURI().toURL()});
    Map<String, List<URL>> streamsetsLibsCl = new LinkedHashMap<>();
    Map<String, List<URL>> userLibsCL = new LinkedHashMap<>();
    streamsetsLibsCl.put("abc123", ImmutableList.copyOf(new URLClassLoader(new URL[]{createJar(new File(streamsetsLibsDir, "abc123"))
      .toURI().toURL()}).getURLs()));
    streamsetsLibsCl.put("abc456", ImmutableList.copyOf(new URLClassLoader(new URL[]{createJar(new File(streamsetsLibsDir, "abc456"))
      .toURI().toURL()}).getURLs()));
    userLibsCL.put("yxz456", ImmutableList.copyOf(new URLClassLoader(new URL[]{createJar(new File(userLibsDir, "yxz456"))
      .toURI().toURL(), createJar(new File(tempDir, "yxz789")).toURI().toURL()}).getURLs()));
    File staticWebDir = new File(tempDir, "static-web-dir");
    Assert.assertTrue(staticWebDir.mkdir());
    createJar(new File(staticWebDir, "subdir"));
      File tarFile = new File(tempDir, "libs.tar.gz");
    TarFileCreator.createLibsTarGz(ImmutableList.copyOf(apiCl.getURLs()), ImmutableList.copyOf(containerCL.getURLs()),
      streamsetsLibsCl, userLibsCL, staticWebDir, tarFile);
    TarInputStream tis = new TarInputStream(new GZIPInputStream(new FileInputStream(tarFile)));
    readDir("api-lib/", tis);
    readJar(tis);
    readDir("container-lib/", tis);
    readJar(tis);
    readDir("streamsets-libs/", tis);
    readDir("streamsets-libs/abc123/", tis);
    readDir("streamsets-libs/abc123/lib/", tis);
    readJar(tis);
    readDir("streamsets-libs/abc456/", tis);
    readDir("streamsets-libs/abc456/lib/", tis);
    readJar(tis);
    readDir("user-libs/", tis);
    readDir("user-libs/yxz456/", tis);
    readDir("user-libs/yxz456/lib/", tis);
    readJar(tis);
    readJar(tis);
    readJar(tis);
    readDir("libs-common-lib/", tis);
  }

  private static void readJar(TarInputStream tis) throws IOException {
    TarEntry fileEntry = readFile(tis);
    byte[] buffer = new byte[8192 * 8];
    int read = IOUtils.read(tis, buffer);
    JarInputStream jar = new JarInputStream(new ByteArrayInputStream(buffer, 0 , read));
    JarEntry entry = jar.getNextJarEntry();
    Assert.assertNotNull(Utils.format("Read {} bytes and found a null entry", read), entry);
    Assert.assertEquals("sample.txt", entry.getName());
    read = IOUtils.read(jar, buffer);
    Assert.assertEquals(FilenameUtils.getBaseName(fileEntry.getName()),
      new String(buffer, 0, read, StandardCharsets.UTF_8));
  }

  private static TarEntry readFile(TarInputStream tis) throws IOException {
    return readDir(null, tis);
  }

  private static TarEntry readDir(String name, TarInputStream tis) throws IOException {
    TarEntry entry = tis.getNextEntry();
    if (name == null) {
      Assert.assertNotNull("Entry should be not null", entry);
    } else {
      Assert.assertNotNull(Utils.format("Entry {} should be not null", name), entry);
      Assert.assertEquals(name, entry.getName());
    }
    return entry;
  }

  private File createJar(File parentDir) throws IOException {
    if (parentDir.isFile()) {
      parentDir.delete();
    }
    parentDir.mkdirs();
    Assert.assertTrue(parentDir.isDirectory());
    String uuid = UUID.randomUUID().toString();
    File jar = new File(parentDir, uuid + ".jar");
    JarOutputStream out = new JarOutputStream(new FileOutputStream(jar));
    JarEntry entry = new JarEntry("sample.txt");
    out.putNextEntry(entry);
    out.write(uuid.getBytes(StandardCharsets.UTF_8));
    out.closeEntry();
    out.close();
    return jar;
  }
}
