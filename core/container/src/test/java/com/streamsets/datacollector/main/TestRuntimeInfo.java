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
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.util.Configuration;

import com.streamsets.lib.security.http.RemoteSSOService;
import dagger.ObjectGraph;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TestRuntimeInfo {

  @Before
  public void before() {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, "target/w");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, "target/x");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, "target/y");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "target/z");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR, "target/R");
  }

  @After
  public void cleanUp() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR);
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR);
    System.getProperties().remove(RuntimeInfo.DATA_COLLECTOR_BASE_HTTP_URL);
  }

  @Test
  public void testInfoCustom() {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, "w");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, "x");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, "y");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "z");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR, "r");

    List<? extends ClassLoader> customCLs = Arrays.asList(new URLClassLoader(new URL[0], null));
    RuntimeInfo info = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), customCLs);
    Assert.assertEquals(System.getProperty("user.dir"), info.getRuntimeDir());
    Assert.assertEquals("w", info.getStaticWebDir());
    Assert.assertEquals("x", info.getConfigDir());
    Assert.assertEquals("y", info.getLogDir());
    Assert.assertEquals("z", info.getDataDir());
    Assert.assertEquals("r", info.getResourcesDir());
    Assert.assertEquals(customCLs, info.getStageLibraryClassLoaders());
    Logger log = Mockito.mock(Logger.class);
    info.log(log);
  }

  @Test
  public void testAttributes() {
    RuntimeInfo info = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(getClass().getClassLoader()));
    Assert.assertFalse(info.hasAttribute("a"));
    info.setAttribute("a", 1);
    Assert.assertTrue(info.hasAttribute("a"));
    Assert.assertEquals(1, (int)info.getAttribute("a"));
    info.removeAttribute("a");
    Assert.assertFalse(info.hasAttribute("a"));
  }

  @Test
  public void testDefaultIdAndBaseHttpUrl() {
    ObjectGraph og  = ObjectGraph.create(RuntimeModule.class);
    RuntimeInfo info = og.get(RuntimeInfo.class);
    Assert.assertEquals("UNDEF", info.getBaseHttpUrl());
  }

  @Test
  public void testMetrics() {
    ObjectGraph og  = ObjectGraph.create(RuntimeModule.class);
    RuntimeInfo info = og.get(RuntimeInfo.class);
    Assert.assertNotNull(info.getMetrics());
  }

  @Test
  public void testConfigIdAndBaseHttpUrl() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, dir.getAbsolutePath());
    Properties props = new Properties();
    props.setProperty(RuntimeInfo.DATA_COLLECTOR_BASE_HTTP_URL, "HTTP");
    props.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "AUTH_TOKEN");
    props.setProperty(RemoteSSOService.DPM_ENABLED, "true");
    props.setProperty(RemoteSSOService.DPM_DEPLOYMENT_ID, "foo");
    Writer writer = new FileWriter(new File(dir, "sdc.properties"));
    props.store(writer, "");
    writer.close();
    ObjectGraph og  = ObjectGraph.create(RuntimeModule.class);
    og.get(Configuration.class);
    RuntimeInfo info = og.get(RuntimeInfo.class);
    Assert.assertEquals("HTTP", info.getBaseHttpUrl());
    Assert.assertEquals("AUTH_TOKEN", info.getAppAuthToken());
    Assert.assertTrue(info.isDPMEnabled());
    Assert.assertEquals("foo", info.getDeploymentId());
  }

  @Test
  public void testSlaveRuntimeInfoConfigAndId() throws Exception {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdirs());
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, dir.getAbsolutePath());
    Properties props = new Properties();
    props.setProperty(RuntimeInfo.DATA_COLLECTOR_BASE_HTTP_URL, "HTTP");
    props.setProperty(ClusterModeConstants.CLUSTER_PIPELINE_REMOTE, "true");
    props.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, "AUTH_TOKEN");
    props.setProperty(RemoteSSOService.DPM_ENABLED, "true");
    props.setProperty(Constants.SDC_ID, "MASTER_ID");
    Writer writer = new FileWriter(new File(dir, "sdc.properties"));
    props.store(writer, "");
    writer.close();
    ObjectGraph og  = ObjectGraph.create(SlaveRuntimeModule.class);
    og.get(Configuration.class);
    RuntimeInfo info = og.get(RuntimeInfo.class);
    Assert.assertTrue(info instanceof SlaveRuntimeInfo);
    Assert.assertEquals("UNDEF", info.getBaseHttpUrl());
    Assert.assertEquals("MASTER_ID", info.getMasterSDCId());
    Assert.assertEquals("AUTH_TOKEN", info.getAppAuthToken());
    Assert.assertTrue(((SlaveRuntimeInfo)info).isRemotePipeline());
    ((SlaveRuntimeInfo)info).setId("ID");
    Assert.assertEquals("ID", info.getId());
    Assert.assertTrue(info.isClusterSlave());
    Assert.assertTrue(info.isDPMEnabled());
  }

  @Test
  public void testTrailingSlashRemovedFromHttpURL() throws Exception {
    RuntimeInfo runtimeInfo = new StandaloneRuntimeInfo(null, null, null);
    runtimeInfo.setBaseHttpUrl("http://localhost:18630");
    Assert.assertEquals("http://localhost:18630", runtimeInfo.getBaseHttpUrl());
    // add a leading forward slash
    runtimeInfo.setBaseHttpUrl("http://localhost:18630/");
    Assert.assertEquals("http://localhost:18630", runtimeInfo.getBaseHttpUrl());
    // add two leading forward slash
    runtimeInfo.setBaseHttpUrl("http://localhost:18630//");
    Assert.assertEquals("http://localhost:18630", runtimeInfo.getBaseHttpUrl());
  }

  @Test
  public void testRuntimeInfoSdcId() {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.STATIC_WEB_DIR, "w");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, "x");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.LOG_DIR, "y");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.RESOURCES_DIR, "r");
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR,
                       new File("target", UUID.randomUUID().toString()).getAbsolutePath());

    List<? extends ClassLoader> customCLs = Arrays.asList(new URLClassLoader(new URL[0], null));
    RuntimeInfo info = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), customCLs);
    info.init();
    String id = info.getId();
    Assert.assertNotNull(id);
    info = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), customCLs);
    info.init();
    Assert.assertEquals(id, info.getId());

    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR,
                       new File("target", UUID.randomUUID().toString()).getAbsolutePath());
    info = new StandaloneRuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(), customCLs);
    info.init();
    Assert.assertNotEquals(id, info.getId());
    Assert.assertFalse(info.isClusterSlave());
  }

}
