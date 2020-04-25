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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.SysInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class TestStatsInfo {

  SysInfo sysInfo = Mockito.mock(SysInfo.class);

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", true);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);

    si.startSystem();
    Assert.assertEquals(1, si.getActiveStats().getUpTime().getMultiplier());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo,1000, false));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertTrue(si.getActiveStats().isDpmEnabled());
    Assert.assertTrue(si.getCollectedStats().isEmpty());

    Assert.assertEquals(1, si.getActiveStats().getUpTime().getMultiplier());
  }

  private RuntimeInfo mockRuntimeInfo(String sdcId, boolean dpmEnabled) {
    RuntimeInfo ret = Mockito.mock(RuntimeInfo.class);
    Mockito.when(ret.getId()).thenReturn(sdcId);
    Mockito.when(ret.getProductName()).thenReturn(RuntimeInfo.SDC_PRODUCT);
    Mockito.when(ret.isDPMEnabled()).thenReturn(dpmEnabled);
    return ret;
  }

  @Test
  public void testRollIfNeededSdcIdChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id2", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id1");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v0");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 10000, false));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("id2", si.getActiveStats().getSdcId());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededVersionChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v0");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 10000, false));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededBuildRepoShaChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha2");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 10000, false));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("sha1", si.getActiveStats().getBuildRepoSha());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testNoRollIfExtraInfoChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "B")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());

    Assert.assertFalse(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 10000, false));
    Mockito.verify(si, Mockito.never()).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("sha1", si.getActiveStats().getBuildRepoSha());
    Assert.assertEquals(0, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededDpmEnabledChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", true);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 10000, false));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertTrue(si.getActiveStats().isDpmEnabled());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollFrequencyPassed() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis() - 2);
    si.getActiveStats().setDpmEnabled(false);

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 1, false));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollForcedRoll() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 1, true));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollNoRoll() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);
    ActiveStats as = si.getActiveStats();

    Assert.assertFalse(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 1000, false));
    Mockito.verify(si, Mockito.never()).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(as, si.getActiveStats());
  }

  @Test
  public void testRollIfNeededRollDiscardOldestCollectedStats() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = new StatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis() - 2);
    si.getActiveStats().setDpmEnabled(false);
    ActiveStats as = si.getActiveStats();
    List<StatsBean> collected = new ArrayList<>();
    for (int i = 0; i < StatsInfo.INTERVALS_TO_KEEP; i++) {
      collected.add(new StatsBean());
    }
    si.setCollectedStats(collected);
    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 1, false));
    List<StatsBean> got = si.getCollectedStats();

    collected.remove(0);
    got.remove(StatsInfo.INTERVALS_TO_KEEP - 1);
    Assert.assertEquals(collected, got);
  }

  @Test
  public void testRollIfNeededSystemInfoVariablesNull() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id2", false);

    for (String fieldToNull : ImmutableList.of("sdcId", "buildRepoSha")) {
      StatsInfo si = new StatsInfo();
      si = Mockito.spy(si);
      Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

      si.getActiveStats().setSdcId("id2");
      si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
      si.getActiveStats().setDataCollectorVersion("v1");
      si.getActiveStats().setBuildRepoSha("sha1");
      si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      si.getActiveStats().setStartTime(System.currentTimeMillis());

      Whitebox.setInternalState(si.getActiveStats(), fieldToNull, null);
      Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, 10000, false));
      Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));

      Assert.assertEquals("id2", si.getActiveStats().getSdcId());
      Assert.assertEquals("sha1", si.getActiveStats().getBuildRepoSha());
      Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
      Assert.assertEquals(ImmutableMap.of("a", "A"), si.getActiveStats().getExtraInfo());
      Assert.assertEquals(1, si.getCollectedStats().size());
    }
  }


}
