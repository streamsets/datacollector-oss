/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.snapshot;

import com.streamsets.dc.execution.SnapshotStore;
import com.streamsets.dc.execution.snapshot.cache.CacheSnapshotStore;
import com.streamsets.dc.execution.snapshot.cache.dagger.CacheSnapshotStoreModule;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestCacheSnapshotStore extends TestSnapshotStore {

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
  }

  @AfterClass
  public static void afterClass() {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    ObjectGraph objectGraph = ObjectGraph.create(CacheSnapshotStoreModule.class);
    snapshotStore = objectGraph.get(SnapshotStore.class);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testCheckStoreImplementation() {
    Assert.assertTrue(snapshotStore instanceof CacheSnapshotStore);
  }

}
