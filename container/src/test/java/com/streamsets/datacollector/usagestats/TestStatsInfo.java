/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.usagestats;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class TestStatsInfo {

  @Test
  public void testGetters() {
    StatsInfo si = new StatsInfo();
    Assert.assertNotNull(si.getActiveStats());
    Assert.assertNotNull(si.getCollectedStats());
  }

  @Test
  public void testCollection() throws Exception {
    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    ActiveStats ai = new ActiveStats();
    ai = Mockito.spy(ai);
    si.setActiveStats(ai);

    si.startSystem();
    Mockito.verify(ai, Mockito.times(1)).startSystem();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));

    Mockito.reset(si);

    si.stopSystem();
    Mockito.verify(ai, Mockito.times(1)).stopSystem();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));

    Mockito.reset(si);

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");

    si.startPipeline(pc);
    Mockito.verify(ai, Mockito.times(1)).startPipeline(Mockito.eq(pc));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));

    Mockito.reset(si);

    si.stopPipeline(pc);
    Mockito.verify(ai, Mockito.times(1)).stopPipeline(Mockito.eq(pc));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));

    Mockito.reset(si);

    si.incrementRecordCount(1);
    Mockito.verify(ai, Mockito.times(1)).incrementRecordCount(Mockito.eq(1L));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
  }

  @Test
  public void testSnapshot() throws Exception {
    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    ActiveStats ai = new ActiveStats();
    ai = Mockito.spy(ai);
    si.setActiveStats(ai);
    ActiveStats ais = new ActiveStats();
    Mockito.doReturn(ais).when(ai).snapshot();
    StatsBean sb = new StatsBean();
    si.setCollectedStats(ImmutableList.of(sb));

    StatsInfo sis = si.snapshot();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
    Assert.assertEquals(ais, sis.getActiveStats());
    Assert.assertEquals(ImmutableList.of(sb), sis.getCollectedStats());
  }

  @Test
  public void testReset() throws Exception {
    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    ActiveStats ai = new ActiveStats();
    ai = Mockito.spy(ai);
    si.setActiveStats(ai);
    ActiveStats ais = new ActiveStats();
    Mockito.doReturn(ais).when(ai).roll();
    StatsBean sb = new StatsBean();
    si.setCollectedStats(ImmutableList.of(sb));

    si.reset();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(ais, si.getActiveStats());
    Assert.assertTrue(si.getCollectedStats().isEmpty());
  }

  @Test
  public void testRollIfNeededNoExistingStats() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);

    si.startSystem();
    Assert.assertEquals(1, si.getActiveStats().getUpTime().getMultiplier());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, 1000));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertTrue(si.getActiveStats().isDpmEnabled());
    Assert.assertTrue(si.getCollectedStats().isEmpty());

    Assert.assertEquals(1, si.getActiveStats().getUpTime().getMultiplier());
  }

  @Test
  public void testRollIfNeededVersionChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    si.getActiveStats().setDataCollectorVersion("v0");
    si.getActiveStats().setStartTime(System.currentTimeMillis());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, 10000));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededDpmEnabledChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, 10000));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertTrue(si.getActiveStats().isDpmEnabled());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollFrequencyPassed() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setStartTime(System.currentTimeMillis() - 2);
    si.getActiveStats().setDpmEnabled(false);

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, 1));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollNoRoll() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);
    ActiveStats as = si.getActiveStats();

    Assert.assertFalse(si.rollIfNeeded(buildInfo, runtimeInfo, 1000));
    Mockito.verify(si, Mockito.never()).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(as, si.getActiveStats());
  }

  @Test
  public void testRollIfNeededRollDiscardOldestCollectedStats() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    
    Mockito.when(runtimeInfo.isDPMEnabled()).thenReturn(false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setStartTime(System.currentTimeMillis() - 2);
    si.getActiveStats().setDpmEnabled(false);
    ActiveStats as = si.getActiveStats();
    List<StatsBean> collected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      collected.add(new StatsBean());
    }
    si.setCollectedStats(collected);
    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, 1));
    List<StatsBean> got = si.getCollectedStats();

    collected.remove(0);
    got.remove(9);
    Assert.assertEquals(collected, got);
  }

}
