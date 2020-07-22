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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.usagestats.TestStatsBean.TestModelStatsBeanExtension;
import com.streamsets.datacollector.util.SysInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestStatsInfo {

  SysInfo sysInfo;
  Activation activation;

  @Before
  public void setup() {
    sysInfo = Mockito.mock(SysInfo.class);
    activation = Mockito.mock(Activation.class);
    MockActivationInfo info = new MockActivationInfo();
    info.additionalInfo = ImmutableMap.of("keyToKeep", "valToKeep", "user.doNotKeep", "shouldNotKeep");
    Mockito.when(activation.getInfo()).thenReturn(info);
  }

  @Test
  public void testGetters() {
    StatsInfo si = createStatsInfo();
    Assert.assertNotNull(si.getActiveStats());
    Assert.assertNotNull(si.getCollectedStats());
  }

  @Test
  public void testCollection() throws Exception {
    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    ActiveStats ai = si.getActiveStats();
    ai = Mockito.spy(ai);
    si.setActiveStats(ai);

    TestModelStatsExtension ext = getTestExtension(si);

    si.startSystem();
    Mockito.verify(ai, Mockito.times(1)).startSystem();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
    Assert.assertEquals(1, ext.getStartSystems());

    Mockito.reset(si);

    si.stopSystem();
    Mockito.verify(ai, Mockito.times(1)).stopSystem();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
    Assert.assertEquals(1, ext.getStopSystems());

    Mockito.reset(si);

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");

    si.startPipeline(pc);
    Mockito.verify(ai, Mockito.times(1)).startPipeline(Mockito.eq(pc));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
    Assert.assertEquals(1, ext.getStartPipelines());

    Mockito.reset(si);

    si.stopPipeline(pc);
    Mockito.verify(ai, Mockito.times(1)).stopPipeline(Mockito.eq(pc));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
    Assert.assertEquals(1, ext.getStopPipelines());

    Mockito.reset(si);

    si.incrementRecordCount(1);
    Mockito.verify(ai, Mockito.times(1)).incrementRecordCount(Mockito.eq(1L));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
  }

  @Test
  public void testSnapshot() throws Exception {
    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    ActiveStats ai = si.getActiveStats();
    ai = Mockito.spy(ai);
    si.setActiveStats(ai);
    ActiveStats ais = createActiveStats(si);
    Mockito.doReturn(ais).when(ai).snapshot();
    StatsBean sb = new StatsBean();
    si.setCollectedStats(ImmutableList.of(sb));

    TestModelStatsExtension ext = getTestExtension(si);
    Assert.assertEquals(1, ext.getSnapshots()); // called once when making safe copies in constructor

    StatsInfo sis = si.snapshot();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(false));
    Assert.assertEquals(ais, sis.getActiveStats());
    Assert.assertEquals(ImmutableList.of(sb), sis.getCollectedStats());
    Assert.assertEquals(2, ext.getSnapshots());
  }

  @Test
  public void testReset() throws Exception {
    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    ActiveStats ai = si.getActiveStats();
    ai = Mockito.spy(ai);
    si.setActiveStats(ai);
    ActiveStats ais = createActiveStats(si);
    Mockito.doReturn(ais).when(ai).roll();
    StatsBean sb = new StatsBean();
    si.setCollectedStats(ImmutableList.of(sb));

    TestModelStatsExtension ext = getTestExtension(si);

    si.reset();
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(ais, si.getActiveStats());
    Assert.assertTrue(si.getCollectedStats().isEmpty());
    Assert.assertEquals(0, ext.getRolls()); // reset throws away the one with rolls = 1
  }

  @Test
  public void testRollIfNeededNoExistingStats() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", true);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);

    TestModelStatsExtension ext = getTestExtension(si);

    si.startSystem();
    Assert.assertEquals(1, si.getActiveStats().getUpTime().getMultiplier());
    Assert.assertEquals(1, ext.getStartSystems());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 1000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

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

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id1");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v0");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 10000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

    Assert.assertEquals("id2", si.getActiveStats().getSdcId());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededVersionChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v0");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 10000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededBuildRepoShaChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha2");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 10000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

    Assert.assertEquals("sha1", si.getActiveStats().getBuildRepoSha());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testNoRollIfExtraInfoChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "B")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

    Assert.assertFalse(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 10000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.never()).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(0, ext.getRolls());

    Assert.assertEquals("sha1", si.getActiveStats().getBuildRepoSha());
    Assert.assertEquals(0, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededDpmEnabledChange() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", true);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 10000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

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

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis() - 2);
    si.getActiveStats().setDpmEnabled(false);
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 1, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollForcedRoll() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 1, true, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(1, ext.getRolls());

    Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
    Assert.assertEquals(1, si.getCollectedStats().size());
  }

  @Test
  public void testRollIfNeededRollNoRoll() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));
    ActiveStats as = si.getActiveStats();

    Assert.assertFalse(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 1000, false, System.currentTimeMillis()));
    Mockito.verify(si, Mockito.never()).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
    Assert.assertEquals(as, si.getActiveStats());
    Assert.assertEquals(0, ext.getRolls());
  }

  @Test
  public void testRollIfNeededRollDiscardOldestCollectedStats() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis() - 2);
    si.getActiveStats().setDpmEnabled(false);
    si.getActiveStats().setActivationInfo(new ActivationInfo(activation));
    List<StatsBean> collected = new ArrayList<>();

    long now = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      StatsBean bean = new StatsBean();
      bean.setEndTime(now - StatsInfo.PERIOD_OF_TIME_TO_KEEP_STATS_IN_MILLIS + i - 5);
      collected.add(bean);
    }

    si.setCollectedStats(collected);
    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 1, false, now));
    Assert.assertEquals(1, ext.getRolls());
    List<StatsBean> got = si.getCollectedStats();

    Assert.assertEquals(6, got.size());
    got.forEach(s -> Assert.assertTrue(now - s.getEndTime() <= StatsInfo.PERIOD_OF_TIME_TO_KEEP_STATS_IN_MILLIS));
  }

  @Test
  public void testRollIfNeededRollWhenActivationChanges() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", false);

    StatsInfo si = createStatsInfo();
    si = Mockito.spy(si);
    Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

    // make sure we are testing filtering
    Assert.assertEquals(2, activation.getInfo().getAdditionalInfo().size());
    Assert.assertTrue(activation.getInfo().getAdditionalInfo().containsKey("user.doNotKeep"));
    ActivationInfo activationInfo = new ActivationInfo(activation);
    Assert.assertEquals(ImmutableMap.of("keyToKeep", "valToKeep"), activationInfo.getAdditionalInfo());

    TestModelStatsExtension ext = getTestExtension(si);

    si.getActiveStats().setSdcId("id");
    si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
    si.getActiveStats().setDataCollectorVersion("v1");
    si.getActiveStats().setBuildRepoSha("sha1");
    si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
    si.getActiveStats().setStartTime(System.currentTimeMillis());
    si.getActiveStats().setDpmEnabled(false);
    si.getActiveStats().setActivationInfo(activationInfo);

    String initialType = si.getActiveStats().getActivationInfo().getActivationType();
    String newType = "NEW_TYPE";
    Assert.assertNotEquals(initialType, newType);

    Activation newActivation = Mockito.mock(Activation.class);
    MockActivationInfo newActivationInfo = new MockActivationInfo();
    newActivationInfo.type = newType;
    Mockito.when(newActivation.getInfo()).thenReturn(newActivationInfo);

    Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, newActivation, 1000, false, System.currentTimeMillis()));
    Assert.assertEquals(1, ext.getRolls());
    Assert.assertEquals("NEW_TYPE", si.getActiveStats().getActivationInfo().getActivationType());
    Assert.assertEquals(initialType, si.getCollectedStats().get(0).getActivationInfo().getActivationType());
    Assert.assertEquals(
            ImmutableSet.of("keyToKeep"),
            si.getCollectedStats().get(0).getActivationInfo().getAdditionalInfo().keySet());
  }

  @Test
  public void testRollIfNeededSystemInfoVariablesNull() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");
    RuntimeInfo runtimeInfo = mockRuntimeInfo("id2", false);

    for (String fieldToNull : ImmutableList.of("sdcId", "buildRepoSha")) {
      StatsInfo si = createStatsInfo();
      si = Mockito.spy(si);
      Mockito.doReturn(ImmutableMap.of("a", "A")).when(si).getExtraInfo(sysInfo);

      TestModelStatsExtension ext = getTestExtension(si);

      si.getActiveStats().setSdcId("id2");
      si.getActiveStats().setProductName(RuntimeInfo.SDC_PRODUCT);
      si.getActiveStats().setDataCollectorVersion("v1");
      si.getActiveStats().setBuildRepoSha("sha1");
      si.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      si.getActiveStats().setStartTime(System.currentTimeMillis());
      si.getActiveStats().setActivationInfo(new ActivationInfo(activation));

      Whitebox.setInternalState(si.getActiveStats(), fieldToNull, null);
      Assert.assertTrue(si.rollIfNeeded(buildInfo, runtimeInfo, sysInfo, activation, 10000, false, System.currentTimeMillis()));
      Mockito.verify(si, Mockito.times(1)).doWithLock(Mockito.any(Runnable.class), Mockito.eq(true));
      Assert.assertEquals(1, ext.getRolls());

      Assert.assertEquals("id2", si.getActiveStats().getSdcId());
      Assert.assertEquals("sha1", si.getActiveStats().getBuildRepoSha());
      Assert.assertEquals("v1", si.getActiveStats().getDataCollectorVersion());
      Assert.assertEquals(ImmutableMap.of("a", "A"), si.getActiveStats().getExtraInfo());
      Assert.assertEquals(1, si.getCollectedStats().size());
    }
  }

  private StatsInfo createStatsInfo() {
    return new StatsInfo(ImmutableList.of(
        new TestModelStatsExtension()));
  }

  /** Warning: some things are weird when creating orphaned ActiveStats objects like this */
  private ActiveStats createActiveStats(StatsInfo si) {
    return new ActiveStats(si.getActiveStats().getExtensions());
  }

  private TestModelStatsExtension getTestExtension(StatsInfo info) {
    Assert.assertEquals(1, info.getActiveStats().getExtensions().size());
    return (TestModelStatsExtension) info.getActiveStats().getExtensions().get(0);
  }

  static class TestModelStatsExtension extends AbstractStatsExtension {
    static final String VERSION = "4.2";

    long instantiateTime = System.currentTimeMillis();
    int startSystems, stopSystems, createPipelines, previewPipelines, startPipelines, stopPipelines, rolls, snapshots;
    List<PreviewStatus> previewStatuses = new ArrayList<>();
    List<PipelineStatus> pipelineStatuses = new ArrayList<>();

    @Override
    public String getVersion() {
      return VERSION;
    }

    public void setVersion(String version) {
      if (!VERSION.equals(version)) {
        throw new RuntimeException("Unexpected version: " + version);
      }
    }

    public long getInstantiateTime() {
      return instantiateTime;
    }

    public void setInstantiateTime(long instantiateTime) {
      this.instantiateTime = instantiateTime;
    }

    public int getStartSystems() {
      return startSystems;
    }

    public void setStartSystems(int startSystems) {
      this.startSystems = startSystems;
    }

    public int getStopSystems() {
      return stopSystems;
    }

    public void setStopSystems(int stopSystems) {
      this.stopSystems = stopSystems;
    }

    public int getCreatePipelines() {
      return createPipelines;
    }

    public void setCreatePipelines(int createsPipelines) {
      this.createPipelines = createsPipelines;
    }

    public int getPreviewPipelines() {
      return previewPipelines;
    }

    public void setPreviewPipelines(int previewPipelines) {
      this.previewPipelines = previewPipelines;
    }

    public int getStartPipelines() {
      return startPipelines;
    }

    public void setStartPipelines(int startPipelines) {
      this.startPipelines = startPipelines;
    }

    public int getStopPipelines() {
      return stopPipelines;
    }

    public void setStopPipelines(int stopPipelines) {
      this.stopPipelines = stopPipelines;
    }

    public int getRolls() {
      return rolls;
    }

    public void setRolls(int rolls) {
      this.rolls = rolls;
    }

    public int getSnapshots() {
      return snapshots;
    }

    public void setSnapshots(int snapshots) {
      this.snapshots = snapshots;
    }

    public List<PreviewStatus> getPreviewStatuses() { return previewStatuses; }

    public void setPreviewStatuses(List<PreviewStatus> previewStatuses) { this.previewStatuses = previewStatuses; }

    public List<PipelineStatus> getPipelineStatuses() { return pipelineStatuses; }

    public void setPipelineStatuses(List<PipelineStatus> pipelineStatuses) { this.pipelineStatuses = pipelineStatuses; }

    @Override
    protected StatsBeanExtension report() {
      TestModelStatsBeanExtension ret = new TestModelStatsBeanExtension();
      // calculate one more field unique to report
      ret.setNetPipelineStarts(getStartPipelines() - getStopPipelines());

      // report other stuff too
      ret.setStartSystems(getStartSystems());
      ret.setStopSystems(getStopSystems());
      ret.setCreatePipelines(getCreatePipelines());
      ret.setStartPipelines(getStartPipelines());
      ret.setCreatePipelines(getCreatePipelines());
      ret.setStopPipelines(getStopPipelines());
      ret.setRolls(getRolls());
      ret.setSnapshots(getSnapshots());

      // make sure this does something, though test class doesn't use it
      String hashedPid = hashPipelineId("pid");
      Assert.assertNotNull(hashedPid);
      Assert.assertNotEquals("pid", hashedPid);

      return ret;
    }

    @Override
    public void startSystem(ActiveStats activeStats) {
      startSystems++;
    }

    @Override
    public void stopSystem(ActiveStats activeStats) {
      stopSystems++;
    }

    @Override
    public void createPipeline(ActiveStats activeStats, String pipelineId) {
      createPipelines++;
    }

    @Override
    public void previewPipeline(ActiveStats activeStats, String pipelineId) {
      previewPipelines++;
    }

    @Override
    public void startPipeline(ActiveStats activeStats, PipelineConfiguration pipeline) {
      startPipelines++;
    }

    @Override
    public void stopPipeline(ActiveStats activeStats, PipelineConfiguration pipeline) {
      stopPipelines++;
    }

    @Override
    protected void previewStatusChanged(PreviewStatus previewStatus, Previewer previewer) {
      previewStatuses.add(previewStatus);
    }

    @Override
    protected void pipelineStatusChanged(PipelineStatus pipelineStatus, PipelineConfiguration conf, Pipeline p) {
      pipelineStatuses.add(pipelineStatus);
    }

    @Override
    public AbstractStatsExtension roll(ActiveStats activeStats) {
      rolls++;
      return new TestModelStatsExtension();
    }

    @Override
    public AbstractStatsExtension snapshot() {
      snapshots++;
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      TestModelStatsExtension snapshot = null;
      try {
        // using json to do a clone
        snapshot = objectMapper.readValue(objectMapper.writeValueAsString(this), TestModelStatsExtension.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return snapshot;
    }
  }

  private static class MockActivationInfo implements Activation.Info {

    String type;
    boolean valid;
    long firstUse;
    long expiration;
    String sdcId;
    String userInfo;
    List<String> validSdcIds;
    Map<String, Object> additionalInfo;

    @Override
    public String getType() {
      return type;
    }

    @Override
    public boolean isValid() {
      return valid;
    }

    @Override
    public long getFirstUse() {
      return firstUse;
    }

    @Override
    public long getExpiration() {
      return expiration;
    }

    @Override
    public String getSdcId() {
      return sdcId;
    }

    @Override
    public String getUserInfo() {
      return userInfo;
    }

    @Override
    public List<String> getValidSdcIds() {
      return validSdcIds;
    }

    @Override
    public Map<String, Object> getAdditionalInfo() {
      return additionalInfo;
    }
  }
}
