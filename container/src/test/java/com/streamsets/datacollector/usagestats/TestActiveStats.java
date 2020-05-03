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
import com.streamsets.pipeline.api.ErrorCode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

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
    Assert.assertNotNull(as.getPipelines());
    Assert.assertNotNull(as.getStages());
    Assert.assertNotNull(as.getCreateToPreview());
    Assert.assertNotNull(as.getCreateToRun());
    Assert.assertEquals(0, getTestExtension(as).getSnapshots());
  }

  @Test
  public void testSetPipelines() {
    ActiveStats as = new ActiveStats(extensions);
    UsageTimer ut = new UsageTimer().setName("p");
    as.setPipelines(ImmutableList.of(ut));
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(ut, as.getPipelines().get(0));
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
    StageConfiguration stageConfiguration = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConfiguration.getLibrary()).thenReturn("l");
    Mockito.when(stageConfiguration.getStageName()).thenReturn("n");
    Mockito.when(pipelineConfiguration.getStages()).thenReturn((List)ImmutableList.of(stageConfiguration));
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
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getLibrary()).thenReturn("lib");
    Mockito.when(stageConf.getStageName()).thenReturn("stage");
    Mockito.when(pc.getStages()).thenReturn(ImmutableList.of(stageConf));

    as.startPipeline(pc);
    Assert.assertEquals(1, getTestExtension(as).getStartPipelines());
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals("lib::stage", as.getStages().get(0).getName());

    as.stopPipeline(pc);
    Assert.assertEquals(1, getTestExtension(as).getStopPipelines());
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(0, as.getPipelines().get(0).getMultiplier());
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

    Pipeline pipeline = Mockito.mock(Pipeline.class);
    Mockito.when(pipeline.getNumOfRunners()).thenReturn(4);

    Pipeline p = Mockito.mock(Pipeline.class);
    Mockito.when(p.getPipelineConf()).thenReturn(pc);

    as.pipelineStatusChanged(PipelineStatus.STARTING, pc, p);
    Assert.assertEquals(ImmutableList.of(PipelineStatus.STARTING), ext.getPipelineStatuses());

    as.pipelineStatusChanged(PipelineStatus.START_ERROR, pc, p);
    Assert.assertEquals(
        ImmutableList.of(
            PipelineStatus.STARTING,
            PipelineStatus.START_ERROR),
        ext.getPipelineStatuses());
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
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getLibrary()).thenReturn("lib");
    Mockito.when(stageConf.getStageName()).thenReturn("stage");
    Mockito.when(pc.getStages()).thenReturn(ImmutableList.of(stageConf));

    as.startPipeline(pc);
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
    Assert.assertEquals(0, as.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());
    Assert.assertEquals(1, as.getErrorCodes().size());
    Assert.assertEquals(1L, as.getErrorCodes().get("ERROR_01").longValue());

    Assert.assertTrue(roll.getStartTime() >= now);
    Assert.assertEquals(0, roll.getEndTime());
    Assert.assertEquals(1, roll.getUpTime().getMultiplier());
    Assert.assertEquals(1, roll.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, roll.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, roll.getRecordCount());
    Assert.assertEquals(0, roll.getErrorCodes().size());


    //Test Stage / Pipeline purged on stop and then next roll
    roll.stopPipeline(pc);
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(1, as.getStages().size());

    roll = roll.roll();
    Assert.assertTrue(roll.getPipelines().isEmpty());
    Assert.assertTrue(roll.getStages().isEmpty());
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
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getLibrary()).thenReturn("lib");
    Mockito.when(stageConf.getStageName()).thenReturn("stage");
    Mockito.when(pc.getStages()).thenReturn(ImmutableList.of(stageConf));

    as.startPipeline(pc);

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
    Assert.assertEquals(1, as.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());

    Assert.assertEquals(startTime, snapshot.getStartTime());
    Assert.assertEquals(0, snapshot.getEndTime());
    Assert.assertEquals(0, snapshot.getUpTime().getMultiplier());
    Assert.assertEquals(0, snapshot.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(0, snapshot.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, snapshot.getRecordCount());
  }

  @Test
  public void testStartStopPipelineMultipleTimes() throws Exception {
    ActiveStats as = new ActiveStats(extensions);
    as.startSystem();

    PipelineConfiguration pc1 = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc1.getPipelineId()).thenReturn("id1");

    PipelineConfiguration pc2 = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc2.getPipelineId()).thenReturn("id2");

    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getLibrary()).thenReturn("lib");
    Mockito.when(stageConf.getStageName()).thenReturn("stage");

    Mockito.when(pc1.getStages()).thenReturn(ImmutableList.of(stageConf));

    Mockito.when(pc2.getStages()).thenReturn(ImmutableList.of(stageConf));


    as.startPipeline(pc1);
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    as.startPipeline(pc1);
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    as.startPipeline(pc2);
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(2, as.getStages().get(0).getMultiplier());

    Assert.assertEquals(3, getTestExtension(as).getStartPipelines());

    as.stopPipeline(pc1);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    as.stopPipeline(pc1);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    as.stopPipeline(pc2);
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());

    Assert.assertEquals(3, getTestExtension(as).getStopPipelines());

    as.startPipeline(pc1);
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(4, getTestExtension(as).getStartPipelines());

    as.stopPipeline(pc1);
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(4, getTestExtension(as).getStopPipelines());


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
    as.setPipelines(ImmutableList.of(
        new UsageTimer().setName("timer1").setAccumulatedTime(23456)));
    as.getPipelines().get(0).start();
    as.getPipelines().get(0).stop();
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
    Assert.assertEquals(1, deserialized.getPipelines().size());
    UsageTimer expectedRunningPipeline = as.getPipelines().get(0);
    UsageTimer actualRunningPipeline = deserialized.getPipelines().get(0);
    Assert.assertEquals(expectedRunningPipeline.getMultiplier(), actualRunningPipeline.getMultiplier());
    // UsageTimer is running, so can't compare and need to use a delta
    assertWithinDelta(expectedRunningPipeline.getAccumulatedTime(), actualRunningPipeline.getAccumulatedTime(), 1000);
    Assert.assertEquals(as.getErrorCodes(), deserialized.getErrorCodes());

    Assert.assertEquals(1, getTestExtension(deserialized).getStartSystems());
    Assert.assertEquals(extensionInitTime, getTestExtension(deserialized).getInstantiateTime());
    Assert.assertEquals(TestStatsInfo.TestModelStatsExtension.VERSION, getTestExtension(deserialized).getVersion());
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
