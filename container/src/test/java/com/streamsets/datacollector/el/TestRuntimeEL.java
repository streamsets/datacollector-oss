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
package com.streamsets.datacollector.el;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import dagger.ObjectGraph;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.InetAddress;
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
  private static File dataDir;
  private static File configDir;
  private static RuntimeInfo runtimeInfo;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws IOException {
    configDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(configDir.mkdirs());
    resourcesDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(resourcesDir.mkdirs());
    dataDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(dataDir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, configDir.getPath());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR, resourcesDir.getPath());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, dataDir.getPath());

  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before()
  public void setUp() {
    runtimeInfo = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX,new MetricRegistry(),
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
    try (OutputStream os = new FileOutputStream(new File(configDir, "sdc.properties"))) {
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
    String sdcFile = new File(runtimeInfo.getConfigDir(), "sdc.properties").getAbsolutePath();
    OutputStream os = new FileOutputStream(sdcFile);

    InputStream is = Thread.currentThread().getContextClassLoader().
      getResourceAsStream(sdcFileName);

    IOUtils.copy(is, os);
    is.close();
  }

  @Test
  public void testLoadResourceRestrictedFailure() throws Exception {
    exception.expect(IllegalArgumentException.class);

    Path fooFile = Paths.get(resourcesDir.getPath(), "foo.txt");
    Files.write(fooFile, "Hello\n".getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(fooFile, ImmutableSet.of(PosixFilePermission.OTHERS_READ));

    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);

    try {
      RuntimeEL.loadResourceRaw("foo.txt", true);
    } finally {
      Files.deleteIfExists(fooFile);
    }
  }

  @Test
  public void testLoadResourceRestrictedSuccess() throws Exception {
    Path fooFile = Paths.get(resourcesDir.getPath(), "foo.txt");
    Files.write(fooFile, "Hello\n".getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(fooFile, ImmutableSet.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE)
    );

    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);

    try {
      RuntimeEL.loadResourceRaw("foo.txt", true);
    } finally {
      Files.deleteIfExists(fooFile);
    }
  }

  @Test
  public void testLoadResource() throws Exception {
    Path fooFile = Paths.get(resourcesDir.getPath(), "foo.txt");
    try {
      Files.write(fooFile, "Hello\n".getBytes(StandardCharsets.UTF_8));
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
      Assert.assertNull(RuntimeEL.loadResourceRaw("bar.txt", false));
      Assert.assertNull(RuntimeEL.loadResourceRaw("bar.txt", true));
      Assert.assertEquals("Hello\n", RuntimeEL.loadResourceRaw("foo.txt", false));
      Assert.assertEquals("Hello", RuntimeEL.loadResource("foo.txt", false));

    } finally {
      Files.deleteIfExists(fooFile);
    }
  }

  @Test
  public void testAuthToken() throws IOException {
    Properties props = new Properties();
    props.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "AUTH_TOKEN");
    Writer writer = new FileWriter(new File(configDir, "sdc.properties"));
    props.store(writer, "");
    writer.close();
    ObjectGraph og  = ObjectGraph.create(RuntimeModule.class);
    og.get(Configuration.class);
    RuntimeInfo info = og.get(RuntimeInfo.class);
    RuntimeEL.loadRuntimeConfiguration(info);
    Assert.assertEquals("AUTH_TOKEN", RuntimeEL.authToken());
  }

  @Test
  public void testAuthTokenNoFile() throws IOException {
    OutputStream sdcProps = new FileOutputStream(new File(runtimeInfo.getConfigDir(), "sdc.properties"));
    sdcProps.close();

    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    Assert.assertEquals(null, RuntimeEL.authToken());
  }

  @Test
  public void testHostname() throws IOException {
    // No configuration, fetch hostname dynamically
    OutputStream sdcProps = new FileOutputStream(new File(runtimeInfo.getConfigDir(), "sdc.properties"));
    sdcProps.close();

    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    Assert.assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), RuntimeEL.hostname());

    // Hostname in configuration file
    sdcProps = new FileOutputStream(new File(runtimeInfo.getConfigDir(), "sdc.properties"));
    IOUtils.write(WebServerTask.HTTP_BIND_HOST + "=sdc.jarcec.net", sdcProps);
    sdcProps.close();

    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    Assert.assertEquals("sdc.jarcec.net", RuntimeEL.hostname());
  }

}
