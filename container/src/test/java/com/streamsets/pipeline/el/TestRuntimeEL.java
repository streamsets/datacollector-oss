/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

public class TestRuntimeEL {

  private static final String SDC_HOME_KEY = "SDC_HOME";
  private static final String EMBEDDED_SDC_HOME_VALUE = "../sdc-embedded";
  private static final String REMOTE_SDC_HOME_VALUE = "../sdc-remote";

  private static RuntimeInfo runtimeInfo;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR, "./target/" + UUID.randomUUID().toString());
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR));
    f.mkdirs();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.CONFIG_DIR);
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

  //@Test
  public void testRuntimeELRemote() throws IOException {
    createSDCFile("sdc-remote-runtime-conf.properties");
    createRuntimeConfigFile("sdc-runtime-conf.properties");
    RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
    Assert.assertEquals(REMOTE_SDC_HOME_VALUE, RuntimeEL.conf(SDC_HOME_KEY));
  }

  private void createSDCFile(String sdcFileName) throws IOException {
    String sdcFile = new File(runtimeInfo.getConfigDir(), "sdc.properties").getAbsolutePath();
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
}
