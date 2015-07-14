/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.snapshot;

import com.streamsets.dataCollector.execution.snapshot.file.FileSnapshotStore;
import com.streamsets.dataCollector.execution.snapshot.file.dagger.FileSnapshotStoreModule;
import org.junit.BeforeClass;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import dagger.ObjectGraph;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestFileSnapshotStore extends TestSnapshotStore {

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
    ObjectGraph objectGraph = ObjectGraph.create(FileSnapshotStoreModule.class);
    snapshotStore = objectGraph.get(FileSnapshotStore.class);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testCheckStoreImplementation() {
    Assert.assertTrue(snapshotStore instanceof FileSnapshotStore);
  }

}
