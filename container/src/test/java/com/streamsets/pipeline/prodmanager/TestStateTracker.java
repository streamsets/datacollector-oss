/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestStateTracker {

  private StateTracker stateTracker;

  @BeforeClass
  public static void beforeClass() throws IOException {
    System.setProperty("pipeline.data.dir", "./target/var");
    File f = new File(System.getProperty("pipeline.data.dir"));
    FileUtils.deleteDirectory(f);
    TestUtil.captureMockStages();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    System.getProperties().remove("pipeline.data.dir");
  }

  @Before()
  public void setUp() {
    RuntimeInfo info = new RuntimeInfo(Arrays.asList(getClass().getClassLoader()));
    stateTracker = new StateTracker(info);
  }

  @After
  public void tearDown() {
    stateTracker.getStateFile().delete();
  }

  @Test
  public void testGetStateBeforeInit() {
    Assert.assertNull(stateTracker.getState());
  }

  @Test
  public void testGetDefaultState() {
    stateTracker.init();
    PipelineState state = stateTracker.getState();

    Assert.assertNotNull(state);
    Assert.assertEquals("1.0", state.getRev());
    Assert.assertEquals(null, state.getMessage());
    Assert.assertEquals(State.STOPPED, state.getState());

  }

  @Test
  public void testSetState() throws PipelineManagerException {
    stateTracker.init();
    stateTracker.setState("xyz", "2.0", State.RUNNING, "Started pipeline");

    PipelineState state = stateTracker.getState();

    Assert.assertNotNull(state);
    Assert.assertEquals("2.0", state.getRev());
    Assert.assertEquals("Started pipeline", state.getMessage());
    Assert.assertEquals(State.RUNNING, state.getState());
  }

  @Test
  public void testGetStateFile() throws PipelineManagerException {
    stateTracker.init();
    Assert.assertTrue(stateTracker.getStateFile().exists());
  }

  @Test
  public void testReinit() {

    stateTracker.init();

    PipelineState state = stateTracker.getState();

    Assert.assertNotNull(state);
    Assert.assertEquals("1.0", state.getRev());
    Assert.assertEquals(null, state.getMessage());
    Assert.assertEquals(State.STOPPED, state.getState());

    stateTracker.init();

    state = stateTracker.getState();

    Assert.assertNotNull(state);
    Assert.assertEquals("1.0", state.getRev());
    Assert.assertEquals(null, state.getMessage());
    Assert.assertEquals(State.STOPPED, state.getState());
  }

  @Test(expected = RuntimeException.class)
  public void testInitInvalidDir() {
    RuntimeInfo info = Mockito.mock(RuntimeInfo.class);
    Mockito.when(info.getDataDir()).thenReturn("\0");
    stateTracker = new StateTracker(info);

    stateTracker.init();

  }

}
