/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store.impl;

import org.junit.Before;
import com.streamsets.datacollector.store.impl.CachePipelineStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;

import com.streamsets.datacollector.util.LockCache;

import dagger.ObjectGraph;

public class TestCachePipelineStoreTask extends TestFilePipelineStoreTask {

  @Override
  @Before
  public void setUp() {
    ObjectGraph dagger = ObjectGraph.create(new Module());
    store = new CachePipelineStoreTask(dagger.get(FilePipelineStoreTask.class), new LockCache<String>());
  }
}
