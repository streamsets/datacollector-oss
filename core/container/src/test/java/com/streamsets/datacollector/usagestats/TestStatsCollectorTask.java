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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.bundles.BundleType;
import com.streamsets.datacollector.bundles.SupportBundle;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TestStatsCollectorTask {

  private File createTestDir() {
    File dir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(dir.mkdir());
    return dir.getAbsoluteFile();
  }

  @Test
  public void testGetters() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();
    config.set(StatsCollectorTask.ROLL_FREQUENCY_CONFIG, 1);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    Assert.assertEquals(buildInfo, task.getBuildInfo());

    Assert.assertEquals(runtimeInfo, task.getRuntimeInfo());

    Assert.assertEquals(TimeUnit.DAYS.toMillis(1), task.getRollFrequencyMillis());

    Assert.assertNull(task.getStatsInfo());

  }

  @Test
  public void testClusterSlave() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    Mockito.when(runtimeInfo.isClusterSlave()).thenReturn(true);

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());
    Assert.assertNotNull(task.getStatsInfo());

    task.stop();
  }

  @Test
  public void testFirstRunAndCommonInitializationAndStopLogic() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());
    Assert.assertNotNull(task.getStatsInfo());

    Mockito.verify(runnable, Mockito.times(1)).run();
    Mockito.verify(scheduler, Mockito.times(1)).scheduleAtFixedRate(
        Mockito.eq(runnable),
        Mockito.eq(60L),
        Mockito.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );

    Future future = Mockito.mock(ScheduledFuture.class);
    Mockito.doReturn(future).when(task).getFuture();

    Assert.assertEquals(1, task.getStatsInfo().getActiveStats().getUpTime().getMultiplier());

    task.stop();
    Mockito.verify(future, Mockito.times(1)).cancel(Mockito.eq(false));

    Mockito.verify(runnable, Mockito.times(2)).run();

    Assert.assertEquals(0, task.getStatsInfo().getActiveStats().getUpTime().getMultiplier());
  }

  @Test
  public void testInitialOptingOut() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());
    task.setActive(false);
    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testInitialOptingIn() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());
    task.setActive(false);
    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedNo() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, false));
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertFalse(task.isActive());
    Assert.assertNotNull(task.getStatsInfo());

    task.stop();

  }

  @Test
  public void testOptedYesNoPriorStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid1() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid2() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, null);
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid3() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableList.of());
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }

  @Test
  public void testOptedInvalid4() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      os.write("foo".getBytes());
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertFalse(task.isOpted());
    Assert.assertFalse(task.isActive());

    task.stop();
  }


  @Test
  public void testOptedYesPriorStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setDataCollectorVersion("v2");
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task = Mockito.spy(task);
    Runnable runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(task).getRunnable();

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //getRunnable() is mocked out to nothing, that is why we get v2 that we read from file
    Assert.assertEquals("v2", task.getStatsInfo().getActiveStats().getDataCollectorVersion());
    task.stop();
  }

  @Test
  public void testRunnable() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, supportBundleManager);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setDataCollectorVersion("v0");
      statsInfo.getCollectedStats().add(new StatsBean());
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task = Mockito.spy(task);

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //verifying we rolled the read stats
    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());

    Mockito.verify(supportBundleManager, Mockito.times(1)).uploadNewBundleFromInstances(
        Mockito.any(List.class),
        Mockito.eq(BundleType.STATS)
    );
    task.stop();
  }

  @Test
  public void testSetActiveNoChange() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, supportBundleManager);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task = Mockito.spy(task);

    task.init();

    Mockito.reset(task);

    task.setActive(task.isActive());

    Mockito.verify(task, Mockito.never()).saveStats();

    task.stop();
  }

  @Test
  public void testSetActiveFromTrueToFalse() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, supportBundleManager);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo();
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      ObjectMapperFactory.get().writeValue(os, statsInfo);
      statsInfo.getCollectedStats().add(new StatsBean());
    }

    task = Mockito.spy(task);

    task.init();

    Mockito.reset(task);

    long start = task.getStatsInfo().getActiveStats().getStartTime();
    Thread.sleep(1);
    task.setActive(false);

    Assert.assertTrue(task.getStatsInfo().getActiveStats().getStartTime() > start);

    Assert.assertFalse(task.isActive());

    try (InputStream is = new FileInputStream(task.getOptFile())) {
      Map map = ObjectMapperFactory.get().readValue(is, Map.class);
      Assert.assertNotNull(map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
      Assert.assertFalse((Boolean) map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
    }
    Mockito.verify(task, Mockito.times(1)).saveStats();

    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());
    task.stop();
  }

  @Test
  public void testSetActiveFromFalseToTrue() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, null);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, false));
    }

    task = Mockito.spy(task);

    task.init();

    Mockito.reset(task);

    long start = task.getStatsInfo().getActiveStats().getStartTime();
    Thread.sleep(1);
    task.setActive(true);

    Assert.assertTrue(task.getStatsInfo().getActiveStats().getStartTime() > start);

    Assert.assertTrue(task.isActive());

    try (InputStream is = new FileInputStream(task.getOptFile())) {
      Map map = ObjectMapperFactory.get().readValue(is, Map.class);
      Assert.assertNotNull(map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
      Assert.assertTrue((Boolean) map.get(StatsCollectorTask.STATS_ACTIVE_KEY));
    }
    Mockito.verify(task, Mockito.times(1)).saveStats();

    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());
    task.stop();
  }

  @Test
  public void testReportStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getId()).thenReturn("id");
    Mockito.when(runtimeInfo.getDataDir()).thenReturn(testDir.getAbsolutePath());

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    StatsCollectorTask task = new StatsCollectorTask(buildInfo, runtimeInfo, config, scheduler, supportBundleManager);

    List<StatsBean> stats = new ArrayList<>();

    SupportBundle bundle = Mockito.mock(SupportBundle.class);
    Mockito.when(supportBundleManager.generateNewBundleFromInstances(Mockito.anyList(), Mockito.eq(BundleType.STATS))).thenReturn(bundle);

    Assert.assertTrue(task.reportStats(stats));
    ArgumentCaptor<List> generatorCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(supportBundleManager, Mockito.times(1)).uploadNewBundleFromInstances(generatorCaptor.capture(), Mockito.eq(BundleType.STATS));
    Assert.assertEquals(1, generatorCaptor.getValue().size());
    Assert.assertEquals(StatsGenerator.class, generatorCaptor.getValue().get(0).getClass());
  }

}
