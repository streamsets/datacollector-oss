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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ExecutionMode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestActiveStats {

  List<AbstractStatsExtension> extensions = ImmutableList.of(
      new TestStatsInfo.TestModelStatsExtension()
  );

  @Test
  public void testNew() {
    ActiveStats as = new ActiveStats(extensions);
    Assert.assertEquals("", as.getDataCollectorVersion());
    Assert.assertTrue(as.getStartTime() <= System.currentTimeMillis());
    Assert.assertEquals(0, as.getEndTime());
    Assert.assertNotNull(as.getUpTime());
    Assert.assertNotNull(as.getPipelineStats());
    Assert.assertNotNull(as.getDeprecatedPipelines());
    Assert.assertNotNull(as.getStages());
    Assert.assertNotNull(as.getCreateToPreview());
    Assert.assertNotNull(as.getCreateToRun());
    Assert.assertEquals(0, getTestExtension(as).getSnapshots());
  }

  @Test
  public void testSetDeprecatedPipelines() {
    ActiveStats as = new ActiveStats(extensions);
    UsageTimer ut = new UsageTimer().setName("p");
    as.setDeprecatedPipelines(ImmutableList.of(ut));
    Assert.assertEquals(1, as.getDeprecatedPipelines().size());
    Assert.assertEquals(ut, as.getDeprecatedPipelines().get(0));
  }

  @Test
  public void testSetCreateToPreview() {
    ActiveStats as = new ActiveStats(extensions);
    Map map = ImmutableMap.of("p1", new FirstPipelineUse().setCreatedOn(1).setFirstUseOn(3));
    as.setCreateToPreview(map);
    Assert.assertEquals(1, as.getCreateToPreview().size());
    Assert.assertEquals(1, as.getCreateToPreview().get("p1").getCreatedOn());
    Assert.assertEquals(3, as.getCreateToPreview().get("p1").getFirstUseOn());
  }

  @Test
  public void testSetCreateToRun() {
    ActiveStats as = new ActiveStats(extensions);
    Map map = ImmutableMap.of("p1", new FirstPipelineUse().setCreatedOn(1).setFirstUseOn(3));
    as.setCreateToRun(map);
    Assert.assertEquals(1, as.getCreateToRun().size());
    Assert.assertEquals(1, as.getCreateToRun().get("p1").getCreatedOn());
    Assert.assertEquals(3, as.getCreateToRun().get("p1").getFirstUseOn());
  }

  @Test
  public void testCreatePipeline() {
    ActiveStats as = new ActiveStats(extensions);
    as.createPipeline("p1");
    Assert.assertEquals(1, getTestExtension(as).getCreatePipelines());
    long currentTime = System.currentTimeMillis();

    Assert.assertTrue(as.getCreateToPreview().containsKey("p1"));
    Assert.assertTrue(as.getCreateToRun().containsKey("p1"));
    assertWithinDelta(currentTime, as.getCreateToPreview().get("p1").getCreatedOn(), 1000);
    Assert.assertEquals(-1, as.getCreateToPreview().get("p1").getFirstUseOn());
    Assert.assertEquals(0, as.getCreateToPreview().get("p1").getStageCount());
    assertWithinDelta(currentTime, as.getCreateToRun().get("p1").getCreatedOn(), 1000);
    Assert.assertEquals(0, as.getCreateToRun().get("p1").getStageCount());
  }

  @Test
  public void testPreviewPipeline() {
    ActiveStats as = new ActiveStats(extensions);
    as.createPipeline("p1");
    Assert.assertEquals(1, getTestExtension(as).getCreatePipelines());
    as.previewPipeline("p1");
    long currentTime = System.currentTimeMillis();
    Assert.assertEquals(1, getTestExtension(as).getPreviewPipelines());

    Assert.assertTrue(as.getCreateToPreview().containsKey("p1"));
    Assert.assertTrue(as.getCreateToRun().containsKey("p1"));
    assertWithinDelta(currentTime, as.getCreateToPreview().get("p1").getCreatedOn(), 1000);
    Assert.assertTrue(as.getCreateToPreview().get("p1").getFirstUseOn() >= as.getCreateToPreview().get("p1").getCreatedOn());
    Assert.assertEquals(0, as.getCreateToPreview().get("p1").getStageCount());
    assertWithinDelta(currentTime, as.getCreateToRun().get("p1").getCreatedOn(), 1000);
    Assert.assertEquals(-1, as.getCreateToRun().get("p1").getFirstUseOn());
    Assert.assertEquals(0, as.getCreateToRun().get("p1").getStageCount());
  }

  @Test
  public void testRunPipeline() {
    ActiveStats as = new ActiveStats(extensions);
    as.createPipeline("p1");
    Assert.assertEquals(1, getTestExtension(as).getCreatePipelines());
    PipelineConfiguration pipelineConfiguration = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pipelineConfiguration.getPipelineId()).thenReturn("p1");
    mockStageConf(pipelineConfiguration, "l", "n");
    as.startPipeline(pipelineConfiguration);
    long currentTime = System.currentTimeMillis();
    Assert.assertEquals(1, getTestExtension(as).getStartPipelines());

    Assert.assertTrue(as.getCreateToPreview().containsKey("p1"));
    Assert.assertTrue(as.getCreateToRun().containsKey("p1"));
    assertWithinDelta(currentTime, as.getCreateToPreview().get("p1").getCreatedOn(), 1000);
    assertWithinDelta(currentTime, as.getCreateToRun().get("p1").getCreatedOn(), 1000);
    Assert.assertEquals(-1, as.getCreateToPreview().get("p1").getFirstUseOn());
    Assert.assertTrue(as.getCreateToRun().get("p1").getFirstUseOn() >= as.getCreateToRun().get("p1").getCreatedOn());
    Assert.assertEquals(1, as.getCreateToRun().get("p1").getStageCount());
  }

  @Test
  public void testSetStages() {
    ActiveStats as = new ActiveStats(extensions);
    UsageTimer ut = new UsageTimer().setName("p");
    as.setStages(ImmutableList.of(ut));
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(ut, as.getStages().get(0));
  }

  @Test
  public void testCollection() throws Exception {
    ActiveStats as = new ActiveStats(extensions);
    as.startSystem();
    Assert.assertEquals(1, as.getUpTime().getMultiplier());
    Assert.assertEquals(1, getTestExtension(as).getStartSystems());
    Thread.sleep(1);
    as.stopSystem();
    Assert.assertEquals(1, getTestExtension(as).getStopSystems());

    Assert.assertEquals(0, as.getUpTime().getMultiplier());

    as.incrementRecordCount(1);
    Assert.assertEquals(1, as.getRecordCount());

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");
    mockStageConf(pc, "lib", "stage");

    long startTime = System.currentTimeMillis();
    as.startPipeline(pc);
    // stats update when pipeline reaches running state
    Assert.assertEquals(1, getTestExtension(as).getStartPipelines());
    Assert.assertEquals(0, as.getDeprecatedPipelines().size());
    Assert.assertEquals(0, as.getPipelineStats().size());
    Assert.assertEquals(0, as.getStages().size());

    Pipeline p = mockPipeline(pc, 7, ExecutionMode.STANDALONE);
    as.pipelineStatusChanged(PipelineStatus.EDITED, pc, p);
    // still not running state
    Assert.assertEquals(
        ImmutableList.of(PipelineStatus.EDITED),
        getTestExtension(as).getPipelineStatuses());
    Assert.assertEquals(0, as.getDeprecatedPipelines().size());
    Assert.assertEquals(0, as.getPipelineStats().size());
    Assert.assertEquals(0, as.getStages().size());

    // Now really run it
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);
    Assert.assertEquals(
        ImmutableList.of(PipelineStatus.EDITED, PipelineStatus.RUNNING),
        getTestExtension(as).getPipelineStatuses());
    Assert.assertEquals(0, as.getDeprecatedPipelines().size());
    PipelineRunStats runStats = getPipelineRunStats(as, "id");
    assertWithinDelta(startTime, runStats.getStartTime(), 1000);
    Assert.assertEquals(1, runStats.getTimer().getMultiplier());
    Assert.assertEquals(7, runStats.getRunnerCount());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), runStats.getExecutionMode());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals("lib::stage", as.getStages().get(0).getName());

    // Timers stop when reaching terminal state, not on beginning of stop
    as.stopPipeline(pc);
    Assert.assertEquals(1, getTestExtension(as).getStopPipelines());
    runStats = getPipelineRunStats(as, "id");
    Assert.assertEquals(1, runStats.getTimer().getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    // Now reach a terminal state
    as.pipelineStatusChanged(PipelineStatus.STOPPED, pc, p);
    Assert.assertEquals(1, getTestExtension(as).getStopPipelines());
    runStats = getPipelineRunStats(as, "id");
    Assert.assertEquals(0, runStats.getTimer().getMultiplier());
    Assert.assertEquals(PipelineStatus.STOPPED.name(), runStats.getFinalState());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
  }

  @Test
  public void testPreviewStatusChange() {
    ActiveStats as = new ActiveStats(extensions);
    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("pid");

    as.createPipeline(pc.getPipelineId());
    TestStatsInfo.TestModelStatsExtension ext = getTestExtension(as);
    Assert.assertEquals(1, ext.getCreatePipelines());

    Previewer previewer = Mockito.mock(Previewer.class);
    as.previewPipeline(pc.getPipelineId());
    Assert.assertEquals(1, ext.getPreviewPipelines());

    as.previewStatusChanged(PreviewStatus.VALIDATING, previewer);
    Assert.assertEquals(ImmutableList.of(PreviewStatus.VALIDATING), ext.getPreviewStatuses());

    as.previewStatusChanged(PreviewStatus.RUN_ERROR, previewer);
    Assert.assertEquals(
        ImmutableList.of(
            PreviewStatus.VALIDATING,
            PreviewStatus.RUN_ERROR),
        ext.getPreviewStatuses());
  }

  @Test
  public void testPipelineStatusChange() {
    ActiveStats as = new ActiveStats(extensions);
    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("pid");

    as.createPipeline(pc.getPipelineId());
    TestStatsInfo.TestModelStatsExtension ext = getTestExtension(as);
    Assert.assertEquals(1, ext.getCreatePipelines());

    as.startPipeline(pc);
    Assert.assertEquals(1, ext.getStartPipelines());

    Pipeline pipeline = mockPipeline(pc, 4, ExecutionMode.STREAMING);

    as.pipelineStatusChanged(PipelineStatus.STARTING, pc, pipeline);
    Assert.assertEquals(ImmutableList.of(PipelineStatus.STARTING), ext.getPipelineStatuses());

    as.pipelineStatusChanged(PipelineStatus.START_ERROR, pc, pipeline);
    Assert.assertEquals(
        ImmutableList.of(
            PipelineStatus.STARTING,
            PipelineStatus.START_ERROR),
        ext.getPipelineStatuses());

    // test transition with null pipeline
    as.pipelineStatusChanged(PipelineStatus.STARTING, pc, null);
    Assert.assertEquals(
        ImmutableList.of(
            PipelineStatus.STARTING,
            PipelineStatus.START_ERROR,
            PipelineStatus.STARTING),
        ext.getPipelineStatuses());
  }

  @Test
  public void testPipelineStatusPartialUpdates() {
    ActiveStats as = new ActiveStats(extensions);

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");

    mockStageConf(pc, "lib", "stage");

    Pipeline p = mockPipeline(pc, 2, ExecutionMode.STANDALONE);

    // don't update stages or pipeline stats on start
    as.startPipeline(pc);
    Assert.assertEquals(0, as.getStages().size());
    Assert.assertEquals(0, as.getPipelineStats().size());

    // populates entry, but not exec mode or runner count
    as.pipelineStatusChanged(PipelineStatus.STARTING, pc, null);
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getPipelineStats().size());
    PipelineRunStats runStats = getPipelineRunStats(as, pc.getPipelineId());
    Assert.assertNull(runStats.getExecutionMode()); // don't trust mode when pipeline is null
    Assert.assertEquals(-1, runStats.getRunnerCount());

    // This should add the runner count and execution mode
    as.pipelineStatusChanged(PipelineStatus.STOPPING, pc, p);
    Assert.assertSame(runStats, getPipelineRunStats(as, pc.getPipelineId()));
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), runStats.getExecutionMode());
    Assert.assertEquals(p.getNumOfRunners(), runStats.getRunnerCount());
    Assert.assertNull(runStats.getFinalState()); // STOPPING isn't terminal

    // Stop without losing runner count
    as.pipelineStatusChanged(PipelineStatus.STOPPED, pc, null);
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, runStats.getTimer().getMultiplier());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), runStats.getExecutionMode());
    Assert.assertEquals(p.getNumOfRunners(), runStats.getRunnerCount());
    Assert.assertEquals(PipelineStatus.STOPPED.name(), runStats.getFinalState());

    // simulate an SDC restart scenario that adds runner count to a running pipeline.
    // initially: No final state, timer stopped
    runStats.setRunnerCount(-1);
    runStats.setFinalState(null);
    // Restart and expect it to come back up, though in practice this p is probably null here (tested above already)
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);
    Assert.assertEquals(1, runStats.getTimer().getMultiplier());
    Assert.assertEquals(p.getNumOfRunners(), runStats.getRunnerCount());
  }

  @Test
  public void testPipelineStatusUpdatesStalePipelines() {
    // StandaloneRunner will pass either null or the last run's Pipeline object with the STARTING event, which is confusing.
    // Make sure we filter it out defensively.

    ActiveStats as = new ActiveStats(extensions);

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");

    mockStageConf(pc, "lib", "stage");

    Pipeline p = mockPipeline(pc, 2, ExecutionMode.STANDALONE);

    as.pipelineStatusChanged(PipelineStatus.STARTING, null, null);
    // no context, can't update anything.
    as.pipelineStatusChanged(PipelineStatus.STARTING, null, null);
    Assert.assertEquals(0, as.getStages().size());
    Assert.assertEquals(0, as.getPipelineStats().size());

    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    PipelineRunStats run1 = getPipelineRunStats(as, pc.getPipelineId());
    Assert.assertEquals(1, run1.getTimer().getMultiplier());
    Assert.assertEquals(2, run1.getRunnerCount());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), run1.getExecutionMode());

    as.pipelineStatusChanged(PipelineStatus.STOPPED, pc, p);
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, run1.getTimer().getMultiplier());

    // create separate objects to ensure pc and p are stale
    PipelineConfiguration pc2 = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc2.getPipelineId()).thenReturn("id");

    mockStageConf(pc2, "lib", "stage");
    Pipeline p2 = mockPipeline(pc2, 3, ExecutionMode.CLUSTER_BATCH);

    // this should only update timers, but not other run state, since STARTING tends to pass stale context objects
    as.pipelineStatusChanged(PipelineStatus.STARTING, pc, p);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getPipelineStats().size());
    PipelineStats pipelineStats = as.getPipelineStats().get(pc2.getPipelineId());
    Assert.assertEquals(2, pipelineStats.getRuns().size());
    PipelineRunStats run2 = pipelineStats.getRuns().get(1);
    Assert.assertNull(run2.getExecutionMode());
    Assert.assertEquals(-1, run2.getRunnerCount());

    // Only updates exec mode
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc2, null);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, run2.getTimer().getMultiplier());
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH.name(), run2.getExecutionMode());
    Assert.assertEquals(-1, run2.getRunnerCount());

    // This should update configs but leave timers alone
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc2, p2);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, run2.getTimer().getMultiplier());
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH.name(), run2.getExecutionMode());
    Assert.assertEquals(3, run2.getRunnerCount());
  }

  @Test
  public void testRoll() throws Exception {
    ActiveStats as = new ActiveStats(extensions);
    as.setDataCollectorVersion("v1");
    as.setBuildRepoSha("sha1");
    as.setExtraInfo(ImmutableMap.of("a", "A"));
    as.setDpmEnabled(true);

    as.startSystem();
    Assert.assertEquals(1, getTestExtension(as).getStartSystems());

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");
    mockStageConf(pc, "lib", "stage");

    Pipeline p = mockPipeline(pc, 7, ExecutionMode.STANDALONE);

    as.startPipeline(pc);
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);
    Assert.assertEquals(1, getTestExtension(as).getStartPipelines());

    as.incrementRecordCount(1);

    as.errorCode(new ErrorCode() {
      @Override
      public String getCode() {
        return "ERROR_01";
      }

      @Override
      public String getMessage() {
        return "ERROR_01";
      }
    });

    long now = System.currentTimeMillis();

    ActiveStats roll = as.roll();
    Assert.assertEquals(1, getTestExtension(as).getRolls());
    Assert.assertTrue(getTestExtension(as).getInstantiateTime() < getTestExtension(roll).getInstantiateTime());

    Assert.assertEquals("v1", as.getDataCollectorVersion());
    Assert.assertEquals("sha1", as.getBuildRepoSha());
    Assert.assertEquals(ImmutableMap.<String, Object>of("a", "A"), as.getExtraInfo());
    Assert.assertTrue(as.isDpmEnabled());

    Assert.assertTrue(as.getEndTime() >= now);
    Assert.assertEquals(0, as.getUpTime().getMultiplier());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());
    Assert.assertEquals(1, as.getErrorCodes().size());
    Assert.assertEquals(1L, as.getErrorCodes().get("ERROR_01").longValue());

    PipelineRunStats runStats = getPipelineRunStats(as, pc.getPipelineId());
    Assert.assertEquals(0, runStats.getTimer().getMultiplier());

    Assert.assertTrue(roll.getStartTime() >= now);
    Assert.assertEquals(0, roll.getEndTime());
    Assert.assertEquals(1, roll.getUpTime().getMultiplier());
    Assert.assertEquals(1, roll.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, roll.getRecordCount());
    Assert.assertEquals(0, roll.getErrorCodes().size());

    PipelineRunStats rollStats = getPipelineRunStats(roll, pc.getPipelineId());
    Assert.assertEquals(runStats.getTimer().getName(), rollStats.getTimer().getName());
    Assert.assertEquals(1, rollStats.getTimer().getMultiplier());
    Assert.assertEquals(7, rollStats.getRunnerCount());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), rollStats.getExecutionMode());
    Assert.assertNull(rollStats.getFinalState());

    Assert.assertEquals(0, as.getDeprecatedPipelines().size());
    Assert.assertEquals(0, roll.getDeprecatedPipelines().size());

    //Test Stage / Pipeline purged on stop and then next roll
    roll.stopPipeline(pc);
    roll.pipelineStatusChanged(PipelineStatus.STOPPED, pc, p);
    Assert.assertEquals(1, as.getPipelineStats().size());
    Assert.assertEquals(0, rollStats.getTimer().getMultiplier());
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());

    roll = roll.roll();
    Assert.assertEquals(0, roll.getPipelineStats().size());
    Assert.assertEquals(0, roll.getStages().size());

    Assert.assertEquals(0, roll.getDeprecatedPipelines().size());
  }

  // Test that roll works if we've deserialized a v1.1 schema
  @Test
  public void testRollV1_1() throws Exception {
    ActiveStats as = new ActiveStats(ImmutableList.of());
    as.setDataCollectorVersion("v1");
    as.setBuildRepoSha("sha1");
    as.setExtraInfo(ImmutableMap.of("a", "A"));
    as.setDpmEnabled(true);

    as.startSystem();

    String pid = "pid";

    as.setDeprecatedPipelines(ImmutableList.of(new UsageTimer()
        .setName(pid)
        .start()));
    as.setStages(ImmutableList.of(new UsageTimer()
        .setName("lib::stage")
        .start()));

    as.setRecordCount(1);

    as.errorCode(new ErrorCode() {
      @Override
      public String getCode() {
        return "ERROR_01";
      }

      @Override
      public String getMessage() {
        return "ERROR_01";
      }
    });

    long now = System.currentTimeMillis();

    ActiveStats roll = as.roll();
    Assert.assertEquals(0, roll.getExtensions().size());

    Assert.assertEquals("v1", as.getDataCollectorVersion());
    Assert.assertEquals("sha1", as.getBuildRepoSha());
    Assert.assertEquals(ImmutableMap.<String, Object>of("a", "A"), as.getExtraInfo());
    Assert.assertTrue(as.isDpmEnabled());

    Assert.assertTrue(as.getEndTime() >= now);
    Assert.assertEquals(0, as.getUpTime().getMultiplier());
    Assert.assertEquals(0, as.getDeprecatedPipelines().get(0).getMultiplier());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());
    Assert.assertEquals(1, as.getErrorCodes().size());
    Assert.assertEquals(1L, as.getErrorCodes().get("ERROR_01").longValue());

    Assert.assertTrue(roll.getStartTime() >= now);
    Assert.assertEquals(0, roll.getEndTime());
    Assert.assertEquals(1, roll.getUpTime().getMultiplier());
    Assert.assertEquals(1, roll.getDeprecatedPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, roll.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, roll.getRecordCount());
    Assert.assertEquals(0, roll.getErrorCodes().size());


    //Test Stage / Pipeline purged on stop and then next roll
    roll.getDeprecatedPipelines().get(0).stop();
    roll.getStages().get(0).stop();
    Assert.assertEquals(1, as.getDeprecatedPipelines().size());
    Assert.assertEquals(1, as.getStages().size());

    roll = roll.roll();
    Assert.assertTrue(roll.getDeprecatedPipelines().isEmpty());
    Assert.assertTrue(roll.getStages().isEmpty());

    Assert.assertEquals(0, roll.getPipelineStats().size());
  }

  @Test
  public void testSnapshot() throws Exception {
    ActiveStats as = new ActiveStats(extensions);
    as.setDataCollectorVersion("v1");
    as.setBuildRepoSha("sha1");
    as.setDpmEnabled(true);
    as.setExtraInfo(ImmutableMap.of("a", "A"));
    as.startSystem();
    long startTime = as.getStartTime();

    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");
    mockStageConf(pc, "lib", "stage");

    Pipeline p = mockPipeline(pc, 3, ExecutionMode.STANDALONE);
    as.startPipeline(pc);
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);

    as.incrementRecordCount(1);

    ActiveStats snapshot = as.snapshot();
    Assert.assertNotSame(getTestExtension(as), getTestExtension(snapshot));
    Assert.assertEquals(1, getTestExtension(as).getSnapshots());
    Assert.assertEquals(1, getTestExtension(snapshot).getSnapshots());
    Assert.assertEquals(getTestExtension(as).getInstantiateTime(), getTestExtension(snapshot).getInstantiateTime());

    Assert.assertEquals("v1", as.getDataCollectorVersion());
    Assert.assertEquals("sha1", as.getBuildRepoSha());
    Assert.assertEquals(ImmutableMap.of("a", "A"), as.getExtraInfo());
    Assert.assertEquals(true, as.isDpmEnabled());

    Assert.assertEquals(startTime, as.getStartTime());
    Assert.assertEquals(0, as.getEndTime());
    Assert.assertEquals(1, as.getUpTime().getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());

    PipelineRunStats runStats = getPipelineRunStats(as, pc.getPipelineId());
    assertWithinDelta(startTime, runStats.getStartTime(), 1000);
    Assert.assertEquals(1, runStats.getTimer().getMultiplier());
    Assert.assertNull(runStats.getFinalState());

    Assert.assertEquals(startTime, snapshot.getStartTime());
    Assert.assertEquals(0, snapshot.getEndTime());
    Assert.assertEquals(0, snapshot.getUpTime().getMultiplier());
    Assert.assertEquals(0, snapshot.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, snapshot.getRecordCount());

    PipelineRunStats snapshotRun = getPipelineRunStats(snapshot, pc.getPipelineId());
    Assert.assertEquals(runStats.getStartTime(), snapshotRun.getStartTime());
    Assert.assertEquals(pc.getPipelineId(), snapshotRun.getTimer().getName());
    Assert.assertEquals(0, snapshotRun.getTimer().getMultiplier());
    assertWithinDelta(1000, snapshotRun.getTimer().getAccumulatedTime(), 1000);
    Assert.assertEquals(3, snapshotRun.getRunnerCount());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), snapshotRun.getExecutionMode());
    Assert.assertNull(snapshotRun.getFinalState());

    Assert.assertEquals(0, as.getDeprecatedPipelines().size());
  }

  // test snapshot behavior for v1.1 schema
  @Test
  public void testSnapshotV1_1() throws Exception {
    ActiveStats as = new ActiveStats(ImmutableList.of());
    as.setDataCollectorVersion("v1");
    as.setBuildRepoSha("sha1");
    as.setDpmEnabled(true);
    as.setExtraInfo(ImmutableMap.of("a", "A"));
    as.startSystem();
    long startTime = as.getStartTime();

    String pid = "pid";

    as.setDeprecatedPipelines(ImmutableList.of(new UsageTimer()
        .setName(pid)
        .start()));
    as.setStages(ImmutableList.of(new UsageTimer()
        .setName("lib::stage")
        .start()));

    as.setRecordCount(1);

    ActiveStats snapshot = as.snapshot();
    Assert.assertEquals(0, as.getExtensions().size());

    Assert.assertEquals("v1", as.getDataCollectorVersion());
    Assert.assertEquals("sha1", as.getBuildRepoSha());
    Assert.assertEquals(ImmutableMap.of("a", "A"), as.getExtraInfo());
    Assert.assertEquals(true, as.isDpmEnabled());

    Assert.assertEquals(startTime, as.getStartTime());
    Assert.assertEquals(0, as.getEndTime());
    Assert.assertEquals(1, as.getUpTime().getMultiplier());
    Assert.assertEquals(1, as.getDeprecatedPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());

    Assert.assertEquals(startTime, snapshot.getStartTime());
    Assert.assertEquals(0, snapshot.getEndTime());
    Assert.assertEquals(0, snapshot.getUpTime().getMultiplier());
    Assert.assertEquals(0, snapshot.getDeprecatedPipelines().get(0).getMultiplier());
    Assert.assertEquals(pid, snapshot.getDeprecatedPipelines().get(0).getName());
    Assert.assertEquals(0, snapshot.getStages().get(0).getMultiplier());
    assertWithinDelta(1000, snapshot.getStages().get(0).getAccumulatedTime(), 1000);
    Assert.assertEquals(1, snapshot.getRecordCount());

    Assert.assertEquals(0, snapshot.getPipelineStats().size());
  }

  @Test
  public void testStartStopPipelineMultipleTimes() throws Exception {
    ActiveStats as = new ActiveStats(extensions);
    as.startSystem();

    PipelineConfiguration pc1 = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc1.getPipelineId()).thenReturn("id1");

    PipelineConfiguration pc2 = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc2.getPipelineId()).thenReturn("id2");

    mockStageConf(pc1, "lib", "stage");
    mockStageConf(pc2, "lib", "stage");

    Pipeline p1 = mockPipeline(pc1, 5, ExecutionMode.STANDALONE);
    Pipeline p2 = mockPipeline(pc2, 7, ExecutionMode.CLUSTER_YARN_STREAMING);

    as.startPipeline(pc1);
    // no-ops
    as.pipelineStatusChanged(PipelineStatus.STARTING, null, null);
    Assert.assertEquals(0, as.getStages().size());
    long start1_1_lower = System.currentTimeMillis();
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc1, p1);
    long start1_1_upper = System.currentTimeMillis();
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    as.startPipeline(pc1);
    as.pipelineStatusChanged(PipelineStatus.STARTING, null, null);
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc1, p1);
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    PipelineStats stats1 = as.getPipelineStats().get(pc1.getPipelineId());
    Assert.assertNotNull(stats1);
    // transitioning to RUNNING again should have been a no-op, one run entry, still running
    Assert.assertEquals(1, stats1.getRuns().size());
    PipelineRunStats run1_1 = stats1.getRuns().get(0);
    Assert.assertTrue(start1_1_lower <= run1_1.getStartTime());
    Assert.assertTrue(start1_1_upper >= run1_1.getStartTime());
    Assert.assertEquals(1, run1_1.getTimer().getMultiplier());
    Assert.assertEquals(5, run1_1.getRunnerCount());
    Assert.assertEquals(ExecutionMode.STANDALONE.name(), run1_1.getExecutionMode());
    Assert.assertNull(run1_1.getFinalState());

    as.startPipeline(pc2);
    as.pipelineStatusChanged(PipelineStatus.STARTING, null, null);
    long start2_lower = System.currentTimeMillis();
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc2, p2);
    long start2_upper = System.currentTimeMillis();
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(2, as.getStages().get(0).getMultiplier());

    Assert.assertEquals(3, getTestExtension(as).getStartPipelines());
    Assert.assertEquals(
        ImmutableList.of(
            PipelineStatus.STARTING, PipelineStatus.RUNNING,
            PipelineStatus.STARTING, PipelineStatus.RUNNING,
            PipelineStatus.STARTING, PipelineStatus.RUNNING
        ),
        getTestExtension(as).getPipelineStatuses());

    as.stopPipeline(pc1);
    long stop1_1_lower = System.currentTimeMillis();
    as.pipelineStatusChanged(PipelineStatus.STOPPED, pc1, p1);
    long stop1_1_upper = System.currentTimeMillis();
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    Assert.assertEquals(0, run1_1.getTimer().getMultiplier());
    Assert.assertTrue(stop1_1_lower - run1_1.getStartTime() <= run1_1.getTimer().getAccumulatedTime());
    Assert.assertTrue(stop1_1_upper - run1_1.getStartTime() >= run1_1.getTimer().getAccumulatedTime());
    Assert.assertEquals(PipelineStatus.STOPPED.name(), run1_1.getFinalState());

    // repeated stop is a no-op, even with a different status change
    as.stopPipeline(pc1);
    as.pipelineStatusChanged(PipelineStatus.STOP_ERROR, pc1, p1);
    Assert.assertSame(stats1, as.getPipelineStats().get(pc1.getPipelineId()));
    Assert.assertEquals(1, stats1.getRuns().size());
    Assert.assertSame(run1_1, stats1.getRuns().get(0));
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, run1_1.getTimer().getMultiplier());
    Assert.assertEquals(PipelineStatus.STOPPED.name(), run1_1.getFinalState());

    as.stopPipeline(pc2);
    long stop2_lower = System.currentTimeMillis();
    as.pipelineStatusChanged(PipelineStatus.STOPPED, pc2, p2);
    long stop2_upper = System.currentTimeMillis();
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    PipelineRunStats run2 = getPipelineRunStats(as, pc2.getPipelineId());
    Assert.assertTrue(start2_lower <= run2.getStartTime());
    Assert.assertTrue(start2_upper >= run2.getStartTime());
    Assert.assertEquals(0, run2.getTimer().getMultiplier());
    Assert.assertTrue(stop2_lower - run2.getStartTime() <= run2.getTimer().getAccumulatedTime());
    Assert.assertTrue(stop2_upper - run2.getStartTime() >= run2.getTimer().getAccumulatedTime());
    Assert.assertEquals(7, run2.getRunnerCount());
    Assert.assertEquals(ExecutionMode.CLUSTER_YARN_STREAMING.name(), run2.getExecutionMode());
    Assert.assertEquals(PipelineStatus.STOPPED.name(), run2.getFinalState());

    Assert.assertEquals(3, getTestExtension(as).getStopPipelines());

    Pipeline p1_2 = mockPipeline(pc1, 3, ExecutionMode.CLUSTER_BATCH);

    as.startPipeline(pc1);
    as.pipelineStatusChanged(PipelineStatus.STARTING, null, null);
    long start1_2_lower = System.currentTimeMillis();
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc1, p1_2);
    long start1_2_upper = System.currentTimeMillis();
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(4, getTestExtension(as).getStartPipelines());

    Assert.assertEquals(2, stats1.getRuns().size());

    PipelineRunStats run1_2 = stats1.getRuns().get(1);
    Assert.assertTrue(start1_2_lower <= run1_2.getStartTime());
    Assert.assertTrue(start1_2_upper >= run1_2.getStartTime());
    Assert.assertEquals(1, run1_2.getTimer().getMultiplier());
    Assert.assertEquals(3, run1_2.getRunnerCount());
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH.name(), run1_2.getExecutionMode());
    Assert.assertNull(run1_2.getFinalState());

    long stop1_2_lower = System.currentTimeMillis();
    as.pipelineStatusChanged(PipelineStatus.RUN_ERROR, pc1, p1_2);
    long stop1_2_upper = System.currentTimeMillis();
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(3, getTestExtension(as).getStopPipelines());
    Assert.assertEquals(0, run1_2.getTimer().getMultiplier());
    Assert.assertTrue(stop1_2_lower - run1_2.getStartTime() <= run1_2.getTimer().getAccumulatedTime());
    Assert.assertTrue(stop1_2_upper - run1_2.getStartTime() >= run1_2.getTimer().getAccumulatedTime());
    Assert.assertEquals(PipelineStatus.RUN_ERROR.name(), run1_2.getFinalState());
  }

  @Test
  public void testRemoveUsedAndExpiredFirstPipelineUse() {
    ActiveStats as = new ActiveStats(extensions);
    Map map = ImmutableMap.of(
        "p1", new FirstPipelineUse().setCreatedOn(100).setFirstUseOn(200),
        "p2", new FirstPipelineUse().setCreatedOn(200),
        "p3", new FirstPipelineUse().setCreatedOn(300)
        );

    map = as.removeUsedAndExpired(map, 250);
    Assert.assertEquals(1, map.size());
    Assert.assertTrue(map.containsKey("p3"));
  }

  @Test
  public void testRestartTimersAfterBootup() throws Exception {
    ActiveStats as = new ActiveStats(extensions);
    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    mockStageConf(pc, "lib", "stage");
    Pipeline p = mockPipeline(pc, 11, ExecutionMode.STANDALONE);
    Mockito.when(pc.getPipelineId()).thenReturn("pid");
    as.createPipeline("pid");
    as.startPipeline(pc);
    as.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);

    // simulate shutdown
    ActiveStats snapshot = as.snapshot();
    ObjectMapper objectMapper = ObjectMapperFactory.get();
    String json = objectMapper.writeValueAsString(snapshot);

    // simulate startup
    ActiveStats deserialized = objectMapper.readValue(json, ActiveStats.class);

    // always roll and report on startup
    ActiveStats bootupRoll = deserialized.roll();
    ActiveStats bootupSnapshot = bootupRoll.snapshot();
    deserialized.startSystem();

    // original should still be running
    PipelineRunStats originalRun = getPipelineRunStats(as, "pid");
    Assert.assertEquals(1, originalRun.getTimer().getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    // our new roll for the latest period should not be running yet, but have no final state
    PipelineRunStats activeRun = getPipelineRunStats(bootupRoll, "pid");
    Assert.assertEquals(0, activeRun.getTimer().getMultiplier());
    // stages get purged since they were stopped on deserialize. They'll come back on start
    Assert.assertEquals(0, bootupRoll.getStages().size());
    Assert.assertEquals(originalRun.getStartTime(), activeRun.getStartTime());
    Assert.assertNull(activeRun.getFinalState());

    // our collected (and probably reported) run should not be running nor have a final state
    PipelineRunStats collectedRun = getPipelineRunStats(deserialized, "pid");
    Assert.assertEquals(0, collectedRun.getTimer().getMultiplier());
    // stages here should NOT have been purged
    Assert.assertEquals(1, deserialized.getStages().size());
    Assert.assertEquals(0, deserialized.getStages().get(0).getMultiplier());
    Assert.assertEquals(originalRun.getStartTime(), collectedRun.getStartTime());
    Assert.assertNull(collectedRun.getFinalState());

    // SDC starts the pipeline back up
    bootupRoll.pipelineStatusChanged(PipelineStatus.RUNNING, pc, p);
    // should still be one run, same instance as before, but now timer is running
    Assert.assertSame(activeRun, getPipelineRunStats(bootupRoll, "pid"));
    Assert.assertEquals(1, activeRun.getTimer().getMultiplier());
    // stages populated again
    Assert.assertEquals(1, bootupRoll.getStages().size());
    Assert.assertEquals(1, bootupRoll.getStages().get(0).getMultiplier());
  }

  @Test
  public void testSerialization() throws Exception {
    ObjectMapper objectMapper = ObjectMapperFactory.get();
    ActiveStats as = new ActiveStats(extensions);
    as.setProductName("FOO");
    as.setExtraInfo(ImmutableMap.of("extraKey", "extraValue"));
    as.setCreateToPreview(ImmutableMap.of(
        "pipelineId1", new FirstPipelineUse()
            .setCreatedOn(System.currentTimeMillis() - 10)
            .setFirstUseOn(System.currentTimeMillis() - 6),
        "pipelineId2", new FirstPipelineUse()
            .setCreatedOn(System.currentTimeMillis() - 5)
    ));
    as.setCreateToRun(ImmutableMap.of(
        "pipelineId3", new FirstPipelineUse()
            .setCreatedOn(System.currentTimeMillis() - 20)
            .setFirstUseOn(System.currentTimeMillis() - 16),
        "pipelineId4", new FirstPipelineUse()
            .setCreatedOn(System.currentTimeMillis() - 15)
    ));

    as.setDeprecatedPipelines(ImmutableList.of(
        new UsageTimer().setName("timer1").setAccumulatedTime(23456)));
    as.getDeprecatedPipelines().get(0).start();
    as.getDeprecatedPipelines().get(0).stop();

    Map<String, PipelineStats> pipelineStats = new ConcurrentHashMap<>();
    pipelineStats.put("pid1", new PipelineStats().setRuns(ImmutableList.of(
        new PipelineRunStats()
            .setStartTime(34567)
            .setTimer(new UsageTimer().setName("pid1").start().stop())
            .setRunnerCount(3)
            .setExecutionMode("execMode1")
    )));
    pipelineStats.put("pid2", new PipelineStats().setRuns(ImmutableList.of(
        new PipelineRunStats()
            .setStartTime(212121)
            .setTimer(new UsageTimer().setName("pid2").start().stop())
            .setRunnerCount(21)
            .setExecutionMode("execMode2.1")
            .setFinalState("finalState2.1"),
        new PipelineRunStats()
            .setStartTime(222222)
            .setTimer(new UsageTimer().setName("pid2").start().stop())
            .setRunnerCount(22)
            .setExecutionMode("execMode2.2")
    )));
    as.setPipelineStats(pipelineStats);

    as.setErrorCodes(ImmutableMap.of("ERROR_1", 3L));

    // configure extension
    as.getExtensions().get(0).startSystem(as);
    Assert.assertEquals(1, getTestExtension(as).getStartSystems());
    long extensionInitTime = getTestExtension(as).getInstantiateTime();
    Assert.assertEquals(TestStatsInfo.TestModelStatsExtension.VERSION, getTestExtension(as).getVersion());

    String json = objectMapper.writeValueAsString(as);
    Assert.assertTrue("json does not contain timer1:" + json, json.contains("timer1")); // check random nested thing
    Assert.assertTrue("json should have extension version: " + json, json.contains(TestStatsInfo.TestModelStatsExtension.VERSION));
    ActiveStats deserialized = objectMapper.readValue(json, ActiveStats.class);

    Assert.assertEquals(as.getProductName(), deserialized.getProductName());
    Assert.assertEquals(as.getExtraInfo(), deserialized.getExtraInfo());
    Assert.assertEquals(as.getCreateToPreview(), deserialized.getCreateToPreview());
    Assert.assertEquals(as.getCreateToRun(), deserialized.getCreateToRun());
    Assert.assertEquals(1, deserialized.getDeprecatedPipelines().size());
    UsageTimer expectedRunningPipeline = as.getDeprecatedPipelines().get(0);
    UsageTimer actualRunningPipeline = deserialized.getDeprecatedPipelines().get(0);
    Assert.assertEquals(0, actualRunningPipeline.getMultiplier());
    Assert.assertEquals(expectedRunningPipeline.getAccumulatedTime(), actualRunningPipeline.getAccumulatedTime());
    Assert.assertEquals(as.getErrorCodes(), deserialized.getErrorCodes());

    PipelineRunStats run1 = getPipelineRunStats(deserialized, "pid1");
    Assert.assertEquals(34567, run1.getStartTime());
    Assert.assertEquals("pid1", run1.getTimer().getName());
    Assert.assertEquals(0, run1.getTimer().getMultiplier());
    Assert.assertEquals(getPipelineRunStats(as, "pid1").getTimer().getAccumulatedTime(), run1.getTimer().getAccumulatedTime());
    Assert.assertEquals(3, run1.getRunnerCount());
    Assert.assertEquals("execMode1", run1.getExecutionMode());
    Assert.assertNull(run1.getFinalState());

    PipelineStats stats2 = deserialized.getPipelineStats().get("pid2");
    Assert.assertNotNull(stats2);
    Assert.assertEquals(2, stats2.getRuns().size());
    PipelineRunStats run2_1 = stats2.getRuns().get(0);
    Assert.assertEquals(212121, run2_1.getStartTime());
    Assert.assertEquals("pid2", run2_1.getTimer().getName());
    Assert.assertEquals(0, run2_1.getTimer().getMultiplier());
    Assert.assertEquals(21, run2_1.getRunnerCount());
    Assert.assertEquals("finalState2.1", run2_1.getFinalState());

    Assert.assertEquals("execMode2.2", stats2.getRuns().get(1).getExecutionMode());

    Assert.assertEquals(1, getTestExtension(deserialized).getStartSystems());
    Assert.assertEquals(extensionInitTime, getTestExtension(deserialized).getInstantiateTime());
    Assert.assertEquals(TestStatsInfo.TestModelStatsExtension.VERSION, getTestExtension(deserialized).getVersion());
  }

  private void mockStageConf(PipelineConfiguration pc, String library, String stageName) {
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getLibrary()).thenReturn(library);
    Mockito.when(stageConf.getStageName()).thenReturn(stageName);

    Mockito.when(pc.getStages()).thenReturn(ImmutableList.of(stageConf));
  }

  private Pipeline mockPipeline(PipelineConfiguration conf, int runnerCount, ExecutionMode mode) {
    Pipeline ret = Mockito.mock(Pipeline.class);
    Mockito.when(ret.getPipelineConf()).thenReturn(conf);
    Mockito.when(ret.getNumOfRunners()).thenReturn(runnerCount);

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.getValue()).thenReturn(mode.name());
    Mockito.when(conf.getConfiguration("executionMode")).thenReturn(config);

    return ret;
  }

  private PipelineRunStats getPipelineRunStats(ActiveStats as, String pipelineId) {
    Assert.assertFalse(as.getPipelineStats().isEmpty());
    PipelineStats pipelineStats = as.getPipelineStats().get(pipelineId);
    Assert.assertNotNull(pipelineStats);
    Assert.assertEquals(1, pipelineStats.getRuns().size());
    return pipelineStats.getRuns().get(0);
  }

  private TestStatsInfo.TestModelStatsExtension getTestExtension(ActiveStats stats) {
    Assert.assertEquals(1, stats.getExtensions().size());
    return (TestStatsInfo.TestModelStatsExtension) stats.getExtensions().get(0);
  }

  private void assertWithinDelta(long expected, long actual, double delta) {
    Assert.assertEquals(
        Long.valueOf(expected).doubleValue(),
        Long.valueOf(actual).doubleValue(),
        delta);
  }
}
