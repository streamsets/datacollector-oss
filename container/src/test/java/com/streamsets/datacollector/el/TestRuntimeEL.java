/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.el;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class TestRuntimeEL {

  private static final String SDC_HOME_KEY = "SDC_HOME";
  private static final String EMBEDDED_SDC_HOME_VALUE = "../sdc-embedded";
  private static final String REMOTE_SDC_HOME_VALUE = "../sdc-remote";

  private static File resourcesDir;
  private static RuntimeInfo runtimeInfo;

  @BeforeClass
  public static void beforeClass() throws IOException {
    File configDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(configDir.mkdirs());
    resourcesDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(resourcesDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, configDir.getPath());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR, resourcesDir.getPath());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR);
  }

  @Before()
  public void setUp() {
    runtimeInfo = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX,new MetricRegistry(),
      Arrays.asList(getClass().getClassLoader()));
  }

  @Test
  public void testRuntimeELEmbedded() throws IOException {
    createSDCFile("sdc-embedded-runtime-conf.properties");
    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    Assert.assertEquals(EMBEDDED_SDC_HOME_VALUE, RuntimeEL.conf(SDC_HOME_KEY));
  }

  @Test
  public void testRuntimeELExternal() throws IOException {
    File configDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(configDir.mkdirs());
    try (OutputStream os = new FileOutputStream(new File(configDir, "dpm.properties"))) {
      Properties props = new Properties();
      props.setProperty("runtime.conf.location", "foo.properties");
      props.store(os, "");
    }
    try (OutputStream os = new FileOutputStream(new File(configDir, "foo.properties"))) {
      Properties props = new Properties();
      props.setProperty("foo", "bar");
      props.store(os, "");
    }
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(configDir.getAbsolutePath());
    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    Assert.assertEquals("bar", RuntimeEL.conf("foo"));
  }

  private void createSDCFile(String sdcFileName) throws IOException {
    String sdcFile = new File(runtimeInfo.getConfigDir(), "dpm.properties").getAbsolutePath();
    OutputStream os = new FileOutputStream(sdcFile);

    InputStream is = Thread.currentThread().getContextClassLoader().
      getResourceAsStream(sdcFileName);

    IOUtils.copy(is, os);
    is.close();
  }

  private void createRuntimeConfigFile(String runtimeConfigFile) throws IOException {
    String sdcFile = new File(runtimeInfo.getConfigDir(), runtimeConfigFile).getAbsolutePath();
    OutputStream os = new FileOutputStream(sdcFile);

    InputStream is = Thread.currentThread().getContextClassLoader().
      getResourceAsStream(runtimeConfigFile);

    IOUtils.copy(is, os);
    is.close();
  }

  @Test
  public void testLoadResource() throws Exception {
    Path fooFile = Paths.get(resourcesDir.getPath(), "foo.txt");
    try {
      Files.write(fooFile, "Hello".getBytes(StandardCharsets.UTF_8));
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
      Assert.assertNull(RuntimeEL.loadResource("bar.txt", false));
      Assert.assertNull(RuntimeEL.loadResource("bar.txt", true));
      Assert.assertEquals("Hello", RuntimeEL.loadResource("foo.txt", false));
      try {
        RuntimeEL.loadResource("foo.txt", true);
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        //nop
      } catch (Exception ex) {
        Assert.fail();
      }
      Files.setPosixFilePermissions(fooFile, ImmutableSet.of(PosixFilePermission.OWNER_READ,
                                                             PosixFilePermission.OWNER_WRITE));
      Assert.assertEquals("Hello", RuntimeEL.loadResource("foo.txt", true));

      try {
        Files.setPosixFilePermissions(fooFile, ImmutableSet.of(PosixFilePermission.OTHERS_READ));
        Assert.assertEquals("Hello", RuntimeEL.loadResource("foo.txt", true));
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        //NOP
      }

      try {
        Files.setPosixFilePermissions(fooFile, ImmutableSet.of(PosixFilePermission.OTHERS_WRITE));
        Assert.assertEquals("Hello", RuntimeEL.loadResource("foo.txt", true));
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        //NOP
      }
    } finally {
      Files.setPosixFilePermissions(fooFile, ImmutableSet.of(PosixFilePermission.OWNER_READ,
                                                             PosixFilePermission.OWNER_WRITE));
    }
  }

}
