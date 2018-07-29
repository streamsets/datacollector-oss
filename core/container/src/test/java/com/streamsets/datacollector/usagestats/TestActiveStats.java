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
import com.streamsets.datacollector.config.StageConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class TestActiveStats {

  @Test
  public void testNew() {
    ActiveStats as = new ActiveStats();
    Assert.assertEquals("", as.getDataCollectorVersion());
    Assert.assertTrue(as.getStartTime() <= System.currentTimeMillis());
    Assert.assertEquals(0, as.getEndTime());
    Assert.assertNotNull(as.getUpTime());
    Assert.assertNotNull(as.getPipelines());
    Assert.assertNotNull(as.getStages());
  }

  @Test
  public void testSetPipelines() {
    ActiveStats as = new ActiveStats();
    UsageTimer ut = new UsageTimer().setName("p");
    as.setPipelines(ImmutableList.of(ut));
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(ut, as.getPipelines().get(0));
  }

  @Test
  public void testSetStages() {
    ActiveStats as = new ActiveStats();
    UsageTimer ut = new UsageTimer().setName("p");
    as.setStages(ImmutableList.of(ut));
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(ut, as.getStages().get(0));
  }

  @Test
  public void testCollection() throws Exception {
    ActiveStats as = new ActiveStats();
    as.startSystem();
    Assert.assertEquals(1, as.getUpTime().getMultiplier());
    Thread.sleep(1);
    as.stopSystem();

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
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(1, as.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());
    Assert.assertEquals("lib::stage", as.getStages().get(0).getName());

    as.stopPipeline(pc);
    Assert.assertEquals(1, as.getPipelines().size());
    Assert.assertEquals(1, as.getStages().size());
    Assert.assertEquals(0, as.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
  }

  @Test
  public void testRoll() throws Exception {
    ActiveStats as = new ActiveStats();

    as.startSystem();


    PipelineConfiguration pc = Mockito.mock(PipelineConfiguration.class);
    Mockito.when(pc.getPipelineId()).thenReturn("id");
    StageConfiguration stageConf = Mockito.mock(StageConfiguration.class);
    Mockito.when(stageConf.getLibrary()).thenReturn("lib");
    Mockito.when(stageConf.getStageName()).thenReturn("stage");
    Mockito.when(pc.getStages()).thenReturn(ImmutableList.of(stageConf));

    as.startPipeline(pc);

    as.incrementRecordCount(1);

    long now = System.currentTimeMillis();
    ActiveStats roll = as.roll();

    Assert.assertTrue(as.getEndTime() >= now);
    Assert.assertEquals(0, as.getUpTime().getMultiplier());
    Assert.assertEquals(0, as.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
    Assert.assertEquals(1, as.getRecordCount());

    Assert.assertTrue(roll.getStartTime() >= now);
    Assert.assertTrue(roll.getEndTime() == 0);
    Assert.assertEquals(1, roll.getUpTime().getMultiplier());
    Assert.assertEquals(1, roll.getPipelines().get(0).getMultiplier());
    Assert.assertEquals(1, roll.getStages().get(0).getMultiplier());
    Assert.assertEquals(0, roll.getRecordCount());
  }

  @Test
  public void testSnapshot() throws Exception {
    ActiveStats as = new ActiveStats();

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
    ActiveStats as = new ActiveStats();
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

    as.stopPipeline(pc1);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    as.stopPipeline(pc1);
    Assert.assertEquals(1, as.getStages().get(0).getMultiplier());

    as.stopPipeline(pc2);
    Assert.assertEquals(0, as.getStages().get(0).getMultiplier());
  }

}
