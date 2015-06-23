/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.dataCollector.execution.PipelineState;
import com.streamsets.dataCollector.execution.PipelineStateStore;
import com.streamsets.dataCollector.execution.PipelineStatus;
import com.streamsets.dataCollector.execution.manager.PipelineStateImpl;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.util.TestUtil;

public class TestFilePipelineStateStore {

  private PipelineStateStore pipelineStateStore;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR, "./target/var");
    TestUtil.captureMockStages();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR);
  }

  @Before()
  public void setUp() throws IOException {
    File f = new File(System.getProperty(RuntimeModule.SDC_PROPERTY_PREFIX + RuntimeInfo.DATA_DIR));
    FileUtils.deleteDirectory(f);
    pipelineStateStore = createStateStore();
    pipelineStateStore.init();
  }

  private static PipelineStateStore createStateStore() {
    RuntimeInfo info = new RuntimeInfo(RuntimeModule.SDC_PROPERTY_PREFIX, new MetricRegistry(),
      Arrays.asList(TestFilePipelineStateStore.class.getClassLoader()));
    com.streamsets.pipeline.util.Configuration configuration = new com.streamsets.pipeline.util.Configuration();
    return new MockFilePipelineStateStore(info, configuration);
  }

  @After
  public void tearDown() {
    pipelineStateStore.destroy();
  }


  public void stateSave() throws Exception {
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.EDITED, "Pipeline edited", null);
    PipelineState pipelineState = pipelineStateStore.getState("aaa", "0");
    assertEquals("user1", pipelineState.getUser());
    assertEquals("aaa", pipelineState.getName());
    assertEquals("0", pipelineState.getRev());
    assertEquals(PipelineStatus.EDITED, pipelineState.getStatus());
    assertEquals("Pipeline edited", pipelineState.getMessage());
  }


  @Test
  public void testStateSaveNoCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = true;
    stateSave();
  }

  @Test
  public void testStateSaveCache() throws Exception {
    MockFilePipelineStateStore.INVALIDATE_CACHE = false;
    stateSave();
  }

  public void stateEdit() throws Exception {
    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null);
    pipelineStateStore.edited("user2", "aaa", "0");
    PipelineState pipelineState = pipelineStateStore.getState("aaa", "0");
    assertEquals("user2", pipelineState.getUser());
    assertEquals("aaa", pipelineState.getName());
    assertEquals("0", pipelineState.getRev());
    assertEquals(PipelineStatus.EDITED, pipelineState.getStatus());

    pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.RUNNING, "Pipeline running", null);
    try {
     pipelineStateStore.edited("user2", "aaa", "0");
     fail("Expected exception but didn't get any");
    } catch (IllegalStateException ex) {
      // expected
    }
  }

   @Test
   public void testStateEditNoCache() throws Exception {
     MockFilePipelineStateStore.INVALIDATE_CACHE = true;
     stateEdit();
   }

   @Test
   public void testStateEditCache() throws Exception {
     MockFilePipelineStateStore.INVALIDATE_CACHE = false;
     stateEdit();
   }

   public void stateDelete() throws Exception {
     pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null);
     pipelineStateStore.delete("aaa", "0");
     try {
       pipelineStateStore.getState("aaa", "0");
       fail("Expected exception but didn't get any");
     } catch (PipelineStoreException ex) {
       // expected
     }
   }

   @Test
   public void testStateDeleteNoCache() throws Exception {
     MockFilePipelineStateStore.INVALIDATE_CACHE = true;
     stateDelete();
   }

   @Test
   public void testStateDeleteCache() throws Exception {
     MockFilePipelineStateStore.INVALIDATE_CACHE = false;
     stateDelete();
   }

   @Test
   public void stateHistory() throws Exception {
     pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.STOPPED, "Pipeline stopped", null);
     pipelineStateStore.saveState("user1", "aaa", "0", PipelineStatus.RUNNING, "Pipeline stopped", null);
     List<PipelineState> history = pipelineStateStore.getHistory("aaa", "0", true);
     for (PipelineState pipelineState: history) {
       assertEquals(PipelineStatus.RUNNING, pipelineState.getStatus());
       assertEquals(PipelineStatus.STOPPED, pipelineState.getStatus());
     }
   }

  static class MockFilePipelineStateStore extends FilePipelineStateStore {

    static boolean INVALIDATE_CACHE = false;
    public MockFilePipelineStateStore(RuntimeInfo runtimeInfo, Configuration conf) {
      super(runtimeInfo, conf);
    }

    @Override
    public void edited(String user, String name, String rev) throws PipelineStoreException {
      super.edited(user, name, rev);
      if (INVALIDATE_CACHE) {
        // invalidate cache
        super.destroy();
      }
    }

    @Override
    public void saveState(String user, String name, String rev, PipelineStatus status, String message,
      Map<String, Object> attributes) throws PipelineStoreException {
      super.saveState(user, name, rev, status, message, attributes);
      if (INVALIDATE_CACHE) {
        super.destroy();
      }
    }

  }

}
