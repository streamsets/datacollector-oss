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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.datacollector.usagestats.TestStatsInfo.TestModelStatsExtension;
import org.junit.Assert;
import org.junit.Test;

public class TestStatsBean {

  @Test
  public void testReported() {
    StatsBean bean = new StatsBean();

    // not reported on creation
    Assert.assertFalse(bean.isReported());

    bean.setReported();
    Assert.assertTrue(bean.isReported());

  }
  @Test
  public void testCreationFromActiveStats() {
    TestModelStatsExtension ext = new TestModelStatsExtension();
    StatsInfo si = new StatsInfo(ImmutableList.of(ext));
    ext.setStatsInfo(si);

    ActiveStats as = new ActiveStats(ImmutableList.of(ext));
    as.setStatsInfo(si);
    as.setDpmEnabled(true);
    as.setDataCollectorVersion("version");
    as.setUpTime(new UsageTimer().setName("upTime").setAccumulatedTime(1));
    as.setStartTime(1);
    as.setEndTime(3);
    as.setRecordCount(1000);
    as.setPipelineStats(ImmutableMap.of(
        "p1", new PipelineStats().setRuns(ImmutableList.of(
            new PipelineRunStats()
                .setStartTime(19)
                .setTimer(new UsageTimer().setName("p1").setAccumulatedTime(1))
                .setRunnerCount(10)
                .setExecutionMode("execMode1")
        )),
        "p2", new PipelineStats().setRuns(ImmutableList.of(
            new PipelineRunStats()
                .setStartTime(10)
                .setTimer(new UsageTimer().setName("p2").setAccumulatedTime(2))
                .setRunnerCount(100)
                .setExecutionMode("execMode2.1")
                .setFinalState("finalState2.1"),
            new PipelineRunStats()
                .setStartTime(16)
                .setTimer(new UsageTimer().setName("p2").setAccumulatedTime(4))
                .setRunnerCount(1000)
                .setExecutionMode("execMode2.2")
        ))
    ));
    String hashedP1 = ext.hashPipelineId("p1");
    String hashedP2 = ext.hashPipelineId("p2");
    Multimap<String, PipelineRunReport> expectedRunReports = ImmutableSetMultimap.of(
        hashedP1, new PipelineRunReport()
            .setHashedId(hashedP1)
            .setStartTime(19)
            .setRunMillis(1)
            .setRunnerCount(10)
            .setExecutionMode("execMode1"),
        hashedP2, new PipelineRunReport()
            .setHashedId(hashedP2)
            .setStartTime(10)
            .setRunMillis(2)
            .setRunnerCount(100)
            .setExecutionMode("execMode2.1")
            .setFinalState("finalState2.1"),
        hashedP2, new PipelineRunReport()
            .setHashedId(hashedP2)
            .setStartTime(16)
            .setRunMillis(4)
            .setRunnerCount(1000)
            .setExecutionMode("execMode2.2")
    );
    as.setStages(ImmutableList.of(
        new UsageTimer().setName("s1").setAccumulatedTime(1),
        new UsageTimer().setName("s2").setAccumulatedTime(2)
    ));
    FirstPipelineUse preview1 = new FirstPipelineUse().setCreatedOn(1).setFirstUseOn(2);
    FirstPipelineUse preview2 = new FirstPipelineUse().setCreatedOn(1);
    as.setCreateToPreview(ImmutableMap.of("p1", preview1, "p2", preview2));
    FirstPipelineUse run1 = new FirstPipelineUse().setCreatedOn(10).setFirstUseOn(20).setStageCount(30);
    FirstPipelineUse run2 = new FirstPipelineUse().setCreatedOn(10);
    as.setCreateToRun(ImmutableMap.of("r1", run1, "r2", run2));

    ext.startPipelines = 5;
    ext.stopPipelines = 2;

    StatsBean sb = new StatsBean("sdcid", as);

    Assert.assertEquals("sdcid", sb.getSdcId());
    Assert.assertEquals("version", sb.getDataCollectorVersion());
    Assert.assertEquals(true, sb.isDpmEnabled());
    Assert.assertEquals(1, sb.getUpTime());
    Assert.assertEquals(1, sb.getStartTime());
    Assert.assertEquals(3, sb.getEndTime());
    Assert.assertEquals(3, sb.getRecordsOM());
    Assert.assertEquals(2, sb.getActivePipelines());
    Assert.assertEquals(7, sb.getPipelineMilliseconds());
    Assert.assertEquals((Long) 1L, sb.getStageMilliseconds().get("s1"));
    Assert.assertEquals((Long) 2L, sb.getStageMilliseconds().get("s2"));
    Assert.assertEquals(1, sb.getCreateToPreview().size());
    Assert.assertEquals(1, sb.getCreateToRun().size());
    Assert.assertEquals(preview1.getCreatedOn(), sb.getCreateToPreview().get(0).getCreatedOn());
    Assert.assertEquals(preview1.getFirstUseOn(), sb.getCreateToPreview().get(0).getFirstUseOn());
    Assert.assertEquals(run1.getCreatedOn(), sb.getCreateToRun().get(0).getCreatedOn());
    Assert.assertEquals(run1.getFirstUseOn(), sb.getCreateToRun().get(0).getFirstUseOn());

    Multimap<String, PipelineRunReport> actualRunReports = HashMultimap.create();
    sb.getPipelineRunReports().forEach(r -> {
      actualRunReports.put(r.getHashedId(), r);
    });
    Assert.assertEquals(expectedRunReports, actualRunReports);

    Assert.assertEquals(1, sb.getExtensions().size());
    Assert.assertEquals(TestModelStatsBeanExtension.class, sb.getExtensions().get(0).getClass());
    TestModelStatsBeanExtension sbExt = (TestModelStatsBeanExtension) sb.getExtensions().get(0);
    Assert.assertEquals(5, sbExt.getStartPipelines());
    Assert.assertEquals(2, sbExt.getStopPipelines());
    Assert.assertEquals(3, sbExt.getNetPipelineStarts());
  }

  @Test
  public void testCreationFromActiveStatsV1_1() {
    ActiveStats as = new ActiveStats(ImmutableList.of());
    as.setDpmEnabled(true);
    as.setDataCollectorVersion("version");
    as.setUpTime(new UsageTimer().setName("upTime").setAccumulatedTime(1));
    as.setStartTime(1);
    as.setEndTime(3);
    as.setRecordCount(1000);
    as.setDeprecatedPipelines(ImmutableList.of(
        new UsageTimer().setName("p1").setAccumulatedTime(1),
        new UsageTimer().setName("p2").setAccumulatedTime(2)
    ));
    as.setStages(ImmutableList.of(
        new UsageTimer().setName("s1").setAccumulatedTime(1),
        new UsageTimer().setName("s2").setAccumulatedTime(2)
    ));
    FirstPipelineUse preview1 = new FirstPipelineUse().setCreatedOn(1).setFirstUseOn(2);
    FirstPipelineUse preview2 = new FirstPipelineUse().setCreatedOn(1);
    as.setCreateToPreview(ImmutableMap.of("p1", preview1, "p2", preview2));
    FirstPipelineUse run1 = new FirstPipelineUse().setCreatedOn(10).setFirstUseOn(20).setStageCount(30);
    FirstPipelineUse run2 = new FirstPipelineUse().setCreatedOn(10);
    as.setCreateToRun(ImmutableMap.of("r1", run1, "r2", run2));

    StatsBean sb = new StatsBean("sdcid", as);

    Assert.assertEquals("sdcid", sb.getSdcId());
    Assert.assertEquals("version", sb.getDataCollectorVersion());
    Assert.assertEquals(true, sb.isDpmEnabled());
    Assert.assertEquals(1, sb.getUpTime());
    Assert.assertEquals(1, sb.getStartTime());
    Assert.assertEquals(3, sb.getEndTime());
    Assert.assertEquals(3, sb.getRecordsOM());
    Assert.assertEquals(2, sb.getActivePipelines());
    Assert.assertEquals(3, sb.getPipelineMilliseconds());
    Assert.assertEquals((Long) 1L, sb.getStageMilliseconds().get("s1"));
    Assert.assertEquals((Long) 2L, sb.getStageMilliseconds().get("s2"));
    Assert.assertEquals(1, sb.getCreateToPreview().size());
    Assert.assertEquals(1, sb.getCreateToRun().size());
    Assert.assertEquals(preview1.getCreatedOn(), sb.getCreateToPreview().get(0).getCreatedOn());
    Assert.assertEquals(preview1.getFirstUseOn(), sb.getCreateToPreview().get(0).getFirstUseOn());
    Assert.assertEquals(run1.getCreatedOn(), sb.getCreateToRun().get(0).getCreatedOn());
    Assert.assertEquals(run1.getFirstUseOn(), sb.getCreateToRun().get(0).getFirstUseOn());
    Assert.assertEquals(0, sb.getExtensions().size());
  }

  // just adds one more field to serialize and overwrites version, but otherwise re-uses TestModelStatsExtension
  public static class TestModelStatsBeanExtension extends TestModelStatsExtension implements StatsBeanExtension {
    public static final String VERSION = "TestModelStatsBeanExtension Version";
    private long netPipelineStarts;

    public long getNetPipelineStarts() {
      return netPipelineStarts;
    }

    public void setNetPipelineStarts(long netPipelineStarts) {
      this.netPipelineStarts = netPipelineStarts;
    }

    @Override
    public String getVersion() {
      return VERSION;
    }

    @Override
    public void setVersion(String version) {
      if (!VERSION.equals(version)) {
        throw new RuntimeException("unexpected version: " + version);
      }
    }
  }
}
