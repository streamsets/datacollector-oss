/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.observerstore.impl;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.main.RuntimeInfo;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestFileObserverStore {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "0";

  private FileObserverStore observerStore = null;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(RuntimeInfo.DATA_DIR, "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    RuntimeInfo info = new RuntimeInfo(ImmutableList.of(getClass().getClassLoader()));
    observerStore = new FileObserverStore(info);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testStoreAndRetrieveAlerts() {
    Assert.assertTrue(observerStore.retrieveAlerts(PIPELINE_NAME, PIPELINE_REV).isEmpty());

    List<AlertDefinition> alerts = new ArrayList<>();
    alerts.add(new AlertDefinition("a1", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a2", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a3", "a1", "a", "2", null, true));

    observerStore.storeAlerts(PIPELINE_NAME, PIPELINE_REV, alerts);

    List<AlertDefinition> actualAlerts = observerStore.retrieveAlerts(PIPELINE_NAME, PIPELINE_REV);

    Assert.assertEquals(alerts.size(), actualAlerts.size());
    for(int i = 0; i < alerts.size(); i++) {
      Assert.assertEquals(alerts.get(i).toString(), actualAlerts.get(i).toString());
    }

  }

}
