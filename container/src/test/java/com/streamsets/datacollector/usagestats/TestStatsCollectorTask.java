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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.usagestats.TestStatsInfo.TestModelStatsExtension;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.SysInfo;
import com.streamsets.lib.security.http.RestClient;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestStatsCollectorTask {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Map<String, Object> DEFAULT_SYS_INFO_MAP = ImmutableMap.of("cloudProvider", SysInfo.UNKNOWN);
  private static final String POST_TELEMETRY_URL = "https://fake-url.com/post/telemetry/here";
  private static final String SDC_313_STATS_JSON_FIXTURE = "/com/streamsets/datacollector/usagestats/sdc3.13.stats.json";
  private static final String TRANSFORMER_315_STATS_JSON_FIXTURE = "/com/streamsets/datacollector/usagestats/transformer3.15.stats.json";
  private static final String TRANSFORMER_315_STATS_JSON_SDC_ID = "7cac8108-d67c-11ea-80e3-414ee1f55860";

  private Runnable runnable;
  private HttpURLConnection[] uploadConnectionHolder = new HttpURLConnection[1];

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

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();
    config.set(AbstractStatsCollectorTask.ROLL_PERIOD_CONFIG, 1);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    Assert.assertEquals(buildInfo, task.getBuildInfo());

    Assert.assertEquals(runtimeInfo, task.getRuntimeInfo());

    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), task.getRollFrequencyMillis());

    Assert.assertNull(task.getStatsInfo());

  }

  @Test
  public void testClusterSlave() {
    File testDir = createTestDir();
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, false));
    }

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
    }

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, null);
    }

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableList.of());
    }

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      os.write("foo".getBytes());
    }

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
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    // report fails so we can easily check collected stats
    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, false);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo(task.provideStatsExtensions());
      statsInfo.getActiveStats().setDataCollectorVersion("v2");
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    // On load, we corrected version info to be the current "v1" when we rolled
    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());
    // collected stats should be left alone though
    Assert.assertEquals(1, task.getStatsInfo().getCollectedStats().size());
    Assert.assertEquals("v2", task.getStatsInfo().getCollectedStats().get(0).getDataCollectorVersion());
    Assert.assertEquals(1, task.getStatsInfo().getActiveStats().getExtensions().size());

    // this effectively checks if the extension's statsInfo reference was populated when loaded from disk.
    Assert.assertNotEquals("somePid", task.getStatsInfo().getActiveStats().getExtensions().get(0).hashPipelineId("somePid"));

    task.stop();
  }

  @Test
  public void testRunnable() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo(task.provideStatsExtensions());
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v0");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      statsInfo.getCollectedStats().add(new StatsBean());
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //verifying we rolled the read stats
    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());

    // verifying StatsInfo is populated correctly on activeStats
    Assert.assertNotEquals("pid", task.getStatsInfo().getActiveStats().hashPipelineId("pid"));

    task.stop();
  }

  @Test
  public void testRunnableReportStatsException() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    // though we pass true here, we will temporarily cause failures in other layers first, before we let it get here
    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    // make it fail on first call
    Mockito.doReturn(false).when(task).reportStats(Mockito.anyList());

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    int expectedRolls = 1;
    int expectedReports = 1;
    int expectedSaves = 1;

    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    long initCompleteTime = System.currentTimeMillis();
    task.getRunnable(false).run();
    // we just rolled, should not roll again
    // we are in back off, should not report
    // always save
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // simulate 1 minute passing
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(1));
    task.getRunnable(false).run();
    // only roll once per hour
    // first back off period is 2 minutes, so we still skip it
    // always save
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // simulate 2 minutes passing since init
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(2));
    task.getRunnable(false).run();
    // only roll once per hour
    // completed back off, should report
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // simulate minutes 3 - 5
    for (int mins = 3; mins <= 5; mins++) {
      Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(mins));
      task.getRunnable(false).run();
      // only roll once per hour
      // still in back off, skip report
      expectedSaves++;
      verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);
    }

    // minute 6, second back off completed
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(6));
    task.getRunnable(false).run();
    // only roll once per hour
    // completed back off, should report
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // rather than simulating all minutes, skip to hour mark, which also exceeds back off
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(60));
    task.getRunnable(false).run();
    // hour's up, roll!
    expectedRolls++;
    // completed back off, should report
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // we aren't consistently using the mock time at all layers, so set this field otherwise we will roll again
    task.getStatsInfo().getActiveStats().setStartTime(task.getCurrentTimeMillis());

    // minute 61: still failing, still in back off
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(61));
    task.getRunnable(false).run();
    // only roll once per hour
    // back off period is 16 minutes, so we still skip it
    // always save
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // force a roll and report, which triggers back off again (for 32 mins)
    task.getRunnable(true).run();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // we aren't consistently using the mock time at all layers, so set this field otherwise we will roll again
    task.getStatsInfo().getActiveStats().setStartTime(task.getCurrentTimeMillis());

    // track when this period started for later
    long interestingPeriodStart = task.getCurrentTimeMillis();

    // back off is now 61 + 32 = 93 minutes, try 92 to make sure it is still backing off after a forced roll
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(92));
    task.getRunnable(false).run();
    // only roll once per hour
    // in back off
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // let the next report go through
    Mockito.when(task.reportStats(Mockito.anyList())).thenCallRealMethod();

    Mockito.when(task.getCurrentTimeMillis()).thenReturn(initCompleteTime + TimeUnit.MINUTES.toMillis(93));
    task.getRunnable(false).run();
    // only roll once per hour
    // just left back off
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // next report in 24 hours from oldest period start. Try 30 mins short of that.
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(
            interestingPeriodStart + TimeUnit.HOURS.toMillis(24) - TimeUnit.MINUTES.toMillis(30));
    task.getRunnable(false).run();
    // way overdue for a roll
    expectedRolls++;
    // not quite 24 hours since oldest period start
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // we aren't consistently using the mock time at all layers, so set this field otherwise we will roll again
    task.getStatsInfo().getActiveStats().setStartTime(task.getCurrentTimeMillis());

    // report after the full 24 hrs
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(interestingPeriodStart + TimeUnit.HOURS.toMillis(24));
    task.getRunnable(false).run();
    // just rolled 30m ago
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    interestingPeriodStart = task.getCurrentTimeMillis();

    // make it fail again
    Mockito.doReturn(false).when(task).reportStats(Mockito.anyList());

    // pump the failure count up super high to trigger maximum back off duration of 1 day
    for (long backOff = 1; backOff < TimeUnit.DAYS.toMinutes(1) ; backOff = backOff << 1) {
      task.getRunnable(true).run();
      expectedRolls++;
      expectedReports++;
      expectedSaves++;
      verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);
    }

    // we aren't consistently using the mock time at all layers, so set this field manually
    task.getStatsInfo().getActiveStats().setStartTime(interestingPeriodStart);

    // 23 hours + 59 minutes later, should not report due to backoff (normally at 99% of a day if no back off)
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(
            interestingPeriodStart + TimeUnit.HOURS.toMillis(23) + TimeUnit.MINUTES.toMillis(59));
    task.getRunnable(false).run();
    expectedRolls++;
    // no report, barely too early
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    // we aren't consistently using the mock time at all layers, so set this field otherwise we will roll again
    task.getStatsInfo().getActiveStats().setStartTime(task.getCurrentTimeMillis());

    // 24 hours later, should report
    Mockito.when(task.getCurrentTimeMillis()).thenReturn(interestingPeriodStart + TimeUnit.HOURS.toMillis(24));
    task.getRunnable(false).run();
    // just rolled
    expectedReports++;
    expectedSaves++;
    verifyRollsReportsSaves(task, expectedRolls, expectedReports, expectedSaves);

    task.stop();
  }

  private void verifyRollsReportsSaves(AbstractStatsCollectorTask task, int expectedRolls, int expectedReports, int expectedSaves) {
    Mockito.verify(task, Mockito.times(expectedRolls)).updateAfterRoll(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyList());
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();
  }

  @Test
  public void testSetActiveNoChange() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    SupportBundleManager supportBundleManager = Mockito.mock(SupportBundleManager.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo(task.provideStatsExtensions());
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Mockito.reset(task);

    task.setActive(task.isActive());

    Mockito.verify(task, Mockito.never()).saveStatsInternal();

    task.stop();
  }

  @Test
  public void testSetActiveFromTrueToFalse() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo(task.provideStatsExtensions());
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      ObjectMapperFactory.get().writeValue(os, statsInfo);
      statsInfo.getCollectedStats().add(new StatsBean());
    }

    task.init();

    Mockito.reset(task);

    long start = task.getStatsInfo().getActiveStats().getStartTime();
    Thread.sleep(1);
    task.setActive(false);

    Assert.assertTrue(task.getStatsInfo().getActiveStats().getStartTime() > start);

    Assert.assertFalse(task.isActive());

    try (InputStream is = new FileInputStream(task.getOptFile())) {
      Map map = ObjectMapperFactory.get().readValue(is, Map.class);
      Assert.assertNotNull(map.get(AbstractStatsCollectorTask.STATS_ACTIVE_KEY));
      Assert.assertFalse((Boolean) map.get(AbstractStatsCollectorTask.STATS_ACTIVE_KEY));
    }
    Mockito.verify(task, Mockito.times(1)).saveStatsInternal();

    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());
    task.stop();
  }

  @Test
  public void testUpgradeFrom313WithOptedTrue() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("upgradeSdcId", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    // report should fail so we can easily inspect collected stats
    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, false);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      // This is copied from a real 3.13.0 install
      IOUtils.write(
          "{\n" +
              "  \"stats.active\" : true,\n" +
              "  \"stats.lastReport\" : 1585760851613\n" +
              "}",
          os);
    }

    FileUtils.copyFile(
        new File(this.getClass().getResource(SDC_313_STATS_JSON_FIXTURE).getPath()),
        task.getStatsFile());

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    ActiveStats activeStats = task.getStatsInfo().getActiveStats();
    long startTimeFromFixture = 1588195985352L;
    // we immediately roll, so start time should be more recent
    Assert.assertTrue(activeStats.getStartTime() > startTimeFromFixture);
    // collected time should match fixture's start time
    Assert.assertEquals(1, task.getStatsInfo().getCollectedStats().size());
    StatsBean collected = task.getStatsInfo().getCollectedStats().get(0);
    Assert.assertEquals(startTimeFromFixture, collected.getStartTime());

    // test newer fields in current stats
    Assert.assertEquals("sha1", activeStats.getBuildRepoSha());
    Assert.assertEquals("upgradeSdcId", activeStats.getSdcId());

    // collected stats should have null for new fields ...
    Assert.assertEquals(null, collected.getBuildRepoSha());

    // ... except sdcId. That is the one field we retroactively populate
    Assert.assertEquals("upgradeSdcId", collected.getSdcId());

    // test upgraded fields
    Assert.assertEquals(ActiveStats.VERSION, activeStats.getVersion());
    Assert.assertEquals("v1", activeStats.getDataCollectorVersion());

    // test new extension
    Assert.assertEquals(1, activeStats.getExtensions().size());
    TestModelStatsExtension ext = (TestModelStatsExtension) activeStats.getExtensions().get(0);
    Assert.assertEquals(0, ext.getRolls()); // didn't exist at time of roll
    Assert.assertEquals(1, ext.getStartSystems());
    Assert.assertEquals(0, ext.getStopSystems());

    // test collected fields were not upgraded and have no extensions
    Assert.assertEquals("1.0", collected.getVersion());
    Assert.assertEquals("3.13.0", collected.getDataCollectorVersion());
    Assert.assertEquals(12193 + 47797, collected.getPipelineMilliseconds());
    Assert.assertEquals(Long.valueOf(59989), collected.getStageMilliseconds().get(
        "streamsets-datacollector-dev-lib::com_streamsets_pipeline_stage_devtest_RandomDataGeneratorSource"));
    Assert.assertEquals(Long.valueOf(59989), collected.getStageMilliseconds().get(
        "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_NullDTarget"));
    Assert.assertEquals(1, collected.getRecordsOM());
    Assert.assertEquals(ImmutableList.of(), collected.getExtensions());

    task.stop();
  }

  @Test
  public void testUpdateAfterRollRemovesInvalidExtensions() {
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo(UUID.randomUUID().toString(), null);

    Configuration config = new Configuration();

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, null, true);
    Mockito.when(task.provideStatsExtensions()).thenReturn(ImmutableList.of()); // no extensions

    StatsInfo stats = new StatsInfo(ImmutableList.of(new TestModelStatsExtension()));
    stats.setCurrentSystemInfo(buildInfo, runtimeInfo, task.getSysInfo(), task.getActivation());
    stats.rollIfNeeded(buildInfo, runtimeInfo, task.getSysInfo(), task.getActivation(),1, true, System.currentTimeMillis());
    // collected stats have extensions
    Assert.assertEquals(1, stats.getCollectedStats().size());
    Assert.assertEquals(1, stats.getCollectedStats().get(0).getExtensions().size());

    StatsInfo upgraded = task.updateAfterRoll(stats);

    // Active Stats extensions purged
    Assert.assertEquals(ImmutableList.of(), upgraded.getActiveStats().getExtensions());
    // collected stats should not get purged
    Assert.assertEquals(1, stats.getCollectedStats().size());
    Assert.assertEquals(1, stats.getCollectedStats().get(0).getExtensions().size());
  }

  @Test
  public void testSetActiveFromFalseToTrue() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(AbstractStatsCollectorTask.STATS_ACTIVE_KEY, false));
    }

    task.init();

    long start = task.getStatsInfo().getActiveStats().getStartTime();
    Assert.assertTrue(task.getStatsInfo().getCollectedStats().isEmpty());


    Thread.sleep(1);
    task.setActive(true);

    Assert.assertTrue(task.getStatsInfo().getActiveStats().getStartTime() > start);

    // we just reported the activation interval, but we keep it as reported (SDC-14937)
    Assert.assertEquals(1, task.getStatsInfo().getCollectedStats().size());

    Assert.assertTrue(task.isActive());

    try (InputStream is = new FileInputStream(task.getOptFile())) {
      Map map = ObjectMapperFactory.get().readValue(is, Map.class);
      Assert.assertNotNull(map.get(AbstractStatsCollectorTask.STATS_ACTIVE_KEY));
      Assert.assertTrue((Boolean) map.get(AbstractStatsCollectorTask.STATS_ACTIVE_KEY));
    }

    Mockito.verify(task, Mockito.times(1)).saveStatsInternal();
    task.stop();
  }

  @Test
  public void testSaveStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTaskAndRunnable(buildInfo, runtimeInfo, config, scheduler);
    final AtomicLong nonForceRuns = new AtomicLong(0);
    final AtomicLong forceRuns = new AtomicLong(0);
    Mockito.when(task.isActive()).thenReturn(true);
    Mockito.when(task.isOpted()).thenReturn(true);

    Mockito.when(task.getRunnable(Mockito.anyBoolean())).then((Answer<Runnable>) invocation -> {
        final Runnable runnable = (Runnable) invocation.callRealMethod();
        boolean isForce = (boolean)invocation.getArguments()[0];
        return () -> {
          long runs = (isForce)? forceRuns.incrementAndGet() : nonForceRuns.incrementAndGet();
          runnable.run();
        };
    });

    task.initTask();
    Assert.assertEquals(0, nonForceRuns.get());
    Assert.assertEquals(1, forceRuns.get());
    Mockito.verify(task).saveStatsInternal();

    task.saveStats();
    Assert.assertEquals(1, nonForceRuns.get());
    Assert.assertEquals(1, forceRuns.get());
    Mockito.verify(task, Mockito.times(2)).saveStatsInternal();
  }


  @Test
  public void testReportStats() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    List<StatsBean> stats = ImmutableList.of(new StatsBean());
    stats.get(0).setActivePipelines(5);

    // not reported yet
    Assert.assertFalse(stats.get(0).isReported());

    Assert.assertTrue(task.reportStats(stats));

    // reported
    Assert.assertTrue(stats.get(0).isReported());

    Mockito.verify(task).postToGetTelemetryUrl(
        Mockito.any(),
        Mockito.eq(ImmutableMap.of(
            AbstractStatsCollectorTask.GET_TELEMETRY_URL_ARG_CLIENT_ID, sdcId,
            AbstractStatsCollectorTask.GET_TELEMETRY_URL_ARG_EXTENSION, AbstractStatsCollectorTask.GET_TELEMETRY_URL_ARG_EXTENSION_JSON)));
    Mockito.verify(task).getHttpURLConnection(new URL(POST_TELEMETRY_URL));
    Mockito.verify(uploadConnectionHolder[0]).getResponseCode();
    List<StatsBean> uploadedStats = OBJECT_MAPPER.readValue(
        ((ByteArrayOutputStream) uploadConnectionHolder[0].getOutputStream()).toByteArray(),
        new TypeReference<List<StatsBean>>(){});
    Assert.assertEquals(1, uploadedStats.size());
    Assert.assertEquals(5, uploadedStats.get(0).getActivePipelines());
  }

  @Test
  public void testReportRawStatsArray() throws Exception {
    // simulates uploading just the array of collected stats
    String expectedSdcId = "expected-sdc-id";
    long testTime = System.currentTimeMillis();
    long periodStartTime = testTime - TimeUnit.HOURS.toMillis(1);
    int netPipelineStarts = 5;

    StatsBean statsBean = createStatsBeanForReportRawStatsTests(
        expectedSdcId, periodStartTime, testTime, netPipelineStarts);

    reportAndVerifyRawStats(OBJECT_MAPPER.writeValueAsString(ImmutableList.of(statsBean)),
        expectedSdcId, periodStartTime, testTime, netPipelineStarts);
  }

  @Test
  public void testReportRawStatsJson() throws Exception {
    // simulates uploading a full stats.json (serialized StatsInfo)
    String expectedSdcId = "expected-sdc-id";
    long testTime = System.currentTimeMillis();
    long periodStartTime = testTime - TimeUnit.HOURS.toMillis(1);
    int netPipelineStarts = 7;

    StatsBean statsBean = createStatsBeanForReportRawStatsTests(
        expectedSdcId, periodStartTime, testTime, netPipelineStarts);

    StatsInfo statsInfo = new StatsInfo(ImmutableList.of(new TestModelStatsExtension()));
    statsInfo.getCollectedStats().add(statsBean);

    reportAndVerifyRawStats(OBJECT_MAPPER.writeValueAsString(statsInfo),
        expectedSdcId, periodStartTime, testTime, netPipelineStarts);
  }

  @Test
  public void testReportRawStatsFromApiResponse() throws Exception {
    // simulates uploading a the response to the system/stats API endpoint
    String expectedSdcId = "expected-sdc-id";
    long testTime = System.currentTimeMillis();
    long periodStartTime = testTime - TimeUnit.HOURS.toMillis(1);
    int netPipelineStarts = 11;

    StatsBean statsBean = createStatsBeanForReportRawStatsTests(
        expectedSdcId, periodStartTime, testTime, netPipelineStarts);

    StatsInfo statsInfo = new StatsInfo(ImmutableList.of(new TestModelStatsExtension()));
    statsInfo.getCollectedStats().add(statsBean);

    Map<String, Object> restResponseMap = ImmutableMap.of(
        "opted", true,
        "active", true,
        "stats", statsInfo
    );

    reportAndVerifyRawStats(OBJECT_MAPPER.writeValueAsString(restResponseMap),
        expectedSdcId, periodStartTime, testTime, netPipelineStarts);
  }

  @Test
  public void testReportTransformerRawStats() throws Exception {
    // simulates uploading transformer stats, which has extension types not known to DataCollector
    long testTime = System.currentTimeMillis();

    String rawStats = Files.asCharSource(
        new File(this.getClass().getResource(TRANSFORMER_315_STATS_JSON_FIXTURE).getPath()), StandardCharsets.UTF_8)
        .read();

    String reportedRawStats = reportAndVerifyRawStatsBasicCalls(rawStats, TRANSFORMER_315_STATS_JSON_SDC_ID);

    // Spot check that we didn't lose any info from the unknown extension class
    List<Map<String, Object>> reportedRawList = OBJECT_MAPPER.readValue(reportedRawStats,
        new TypeReference<List<Map<String, Object>>>(){});
    Assert.assertEquals(4, reportedRawList.size());
    Map<String, Object> interestingEntry = reportedRawList.get(1);
    Assert.assertTrue(interestingEntry.containsKey("extensions"));
    List<Map<String, Object>> extensions = (List<Map<String, Object>>) interestingEntry.get("extensions");
    Assert.assertEquals(1, extensions.size());
    Map<String, Object> extension = extensions.get(0);
    Assert.assertEquals("com.streamsets.datatransformer.usagestats.TransformerStatsBeanExtension",
        extension.get("class"));
    List<Map<String, Object>> pipelineRunReports = (List<Map<String, Object>>) extension.get("pipelineRunReports");
    Assert.assertEquals(1, pipelineRunReports.size());
    Assert.assertEquals("LOCAL", pipelineRunReports.get(0).get("clusterType"));
  }

  @Test
  public void testReportRawStatsOptedOutError() throws Exception {
    String rawStats = OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(
        "opted", false,
        "active", false
    ));
    try {
      reportAndVerifyRawStatsBasicCalls(rawStats, null);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("No stats provided"));
    }
  }

  private StatsBean createStatsBeanForReportRawStatsTests(String sdcId, long startTime, long endTime, int netPipelineStarts) {
    TestStatsBean.TestModelStatsBeanExtension beanExtension = new TestStatsBean.TestModelStatsBeanExtension();
    beanExtension.setNetPipelineStarts(netPipelineStarts);

    StatsBean statsBean = new StatsBean();
    statsBean.setSdcId(sdcId);
    statsBean.setStartTime(startTime);
    statsBean.setEndTime(endTime);
    statsBean.setExtensions(ImmutableList.of(beanExtension));

    return statsBean;
  }

  private void reportAndVerifyRawStats(String rawStats, String expectedSdcId, long expectedStartTime,
      long expectedEndTime, int expectedNetPipelineStarts) throws Exception {
    String rawOutput = reportAndVerifyRawStatsBasicCalls(rawStats, expectedSdcId);
    List<StatsBean> reportedBeans = OBJECT_MAPPER.readValue(rawOutput, new TypeReference<List<StatsBean>>(){});
    Assert.assertEquals(1, reportedBeans.size());
    StatsBean reportedBean = reportedBeans.get(0);
    Assert.assertEquals(expectedSdcId, reportedBean.getSdcId());
    Assert.assertEquals(expectedStartTime, reportedBean.getStartTime());
    Assert.assertEquals(expectedEndTime, reportedBean.getEndTime());
    Assert.assertEquals(1, reportedBean.getExtensions().size());
    Assert.assertTrue(reportedBean.getExtensions().get(0) instanceof TestStatsBean.TestModelStatsBeanExtension);
    TestStatsBean.TestModelStatsBeanExtension reportedBeanExtension =
        (TestStatsBean.TestModelStatsBeanExtension) reportedBean.getExtensions().get(0);
    Assert.assertEquals(expectedNetPipelineStarts, reportedBeanExtension.getNetPipelineStarts());
  }

  private String reportAndVerifyRawStatsBasicCalls(String rawStats, String expectedSdcId) throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");

    String sdcId = "should-not-be-used";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, null, true);

    task.reportStats(null, rawStats);

    ArgumentCaptor<Object> getUrlDataCaptor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(task).postToGetTelemetryUrl(Mockito.any(), getUrlDataCaptor.capture());

    Map<String, String> getUrlData = (Map<String, String>) getUrlDataCaptor.getValue();
    Assert.assertEquals(
        ImmutableMap.of(
            AbstractStatsCollectorTask.GET_TELEMETRY_URL_ARG_CLIENT_ID, expectedSdcId,
            AbstractStatsCollectorTask.GET_TELEMETRY_URL_ARG_EXTENSION, AbstractStatsCollectorTask.GET_TELEMETRY_URL_ARG_EXTENSION_JSON),
        getUrlData);

    ByteArrayOutputStream outputStream = (ByteArrayOutputStream) uploadConnectionHolder[0].getOutputStream();
    return outputStream.toString(StandardCharsets.UTF_8.name());
  }

  @Test
  public void testRunnableMultipleRollsAndReport() {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("abc");
    Mockito.when(buildInfo.getBuiltDate()).thenReturn(new Date().toString());
    Mockito.when(buildInfo.getBuiltBy()).thenReturn("System");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);
    Mockito.when(task.isActive()).thenReturn(true);
    SysInfo sysInfo = task.getSysInfo();

    StatsInfo statsInfo = Mockito.spy(new StatsInfo(task.provideStatsExtensions()));
    Mockito.when(task.getStatsInfo()).thenReturn(statsInfo);
    statsInfo.setCurrentSystemInfo(buildInfo, runtimeInfo, sysInfo, task.getActivation());

    long rollFrequencyMillis = task.getRollFrequencyMillis();
    Assert.assertEquals(TimeUnit.HOURS.toMillis(AbstractStatsCollectorTask.ROLL_PERIOD_CONFIG_MAX), rollFrequencyMillis);
    int expectedRolls = 0;
    int expectedReports = 0;
    int expectedSaves = 0;

    // first run, do initial roll and report with force=true
    task.initTask();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();

    // run again, should just save now that we don't force
    Runnable runnable = task.getRunnable(false);
    runnable.run();
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();

    // run after roll period but before report period
    Mockito.when(task.getRollFrequencyMillis()).thenReturn(0L);
    runnable.run();
    expectedRolls++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();

    // run after report period
    Mockito.when(task.getReportPeriodSeconds()).thenReturn(0L);
    runnable.run();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();

    // reset periods and make sure we only save
    Mockito.when(task.getRollFrequencyMillis()).thenReturn(
        TimeUnit.HOURS.toMillis(AbstractStatsCollectorTask.ROLL_PERIOD_CONFIG_MAX));
    Mockito.when(task.getReportPeriodSeconds()).thenReturn(
        (long) AbstractStatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT);
    runnable.run();
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();

    // stop task and make sure we do one full set of activity
    // we didn't properly start statsInfo, so fake the stop call else it will throw
    Mockito.doNothing().when(statsInfo).stopSystem();
    task.stopTask();
    expectedRolls++;
    expectedReports++;
    expectedSaves++;
    Mockito.verify(statsInfo, Mockito.times(expectedRolls)).setActiveStats(Mockito.any());
    Mockito.verify(task, Mockito.times(expectedReports)).reportStats(Mockito.anyListOf(StatsBean.class));
    Mockito.verify(task, Mockito.times(expectedSaves)).saveStatsInternal();
    Mockito.verify(statsInfo).stopSystem();
  }

  @Test
  public void testSkipSnapshotTelemetry() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1-SNAPSHOT");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    List<StatsBean> stats = ImmutableList.of(new StatsBean());
    stats.get(0).setActivePipelines(5);

    Assert.assertTrue(task.reportStats(stats));

    Mockito.verify(task, Mockito.never()).postToGetTelemetryUrl(
        Mockito.any(),
        Mockito.any());
    Mockito.verify(task, Mockito.never()).getHttpURLConnection(Mockito.any());
  }

  @Test
  public void testEnableSnapshotTelemetry() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1-SNAPSHOT");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();
    config.set(AbstractStatsCollectorTask.TELEMETRY_FOR_SNAPSHOT_BUILDS, true);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);

    List<StatsBean> stats = ImmutableList.of(new StatsBean());

    Assert.assertTrue(task.reportStats(stats));

    Mockito.verify(task).postToGetTelemetryUrl(
        Mockito.any(),
        Mockito.any());
    Mockito.verify(task).getHttpURLConnection(Mockito.any());
  }

  @Test
  public void testReportFrequencySecondConfiguration() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("abc");
    Mockito.when(buildInfo.getBuiltDate()).thenReturn(new Date().toString());
    Mockito.when(buildInfo.getBuiltBy()).thenReturn("System");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();
    config.set(AbstractStatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS, 120);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);
    task.initTask();
    Mockito.verify(scheduler).scheduleAtFixedRate(
        Matchers.any(Runnable.class),
        Matchers.eq(60L),
        Matchers.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );
    Assert.assertEquals(120, task.getReportPeriodSeconds());

    scheduler = Mockito.mock(SafeScheduledExecutorService.class);
    //Set it to 48 hours - max at 24 hours
    config.set(AbstractStatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS, TimeUnit.DAYS.toSeconds(2));

    task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);
    task.initTask();
    Mockito.verify(scheduler).scheduleAtFixedRate(
        Matchers.any(Runnable.class),
        Matchers.eq(60L),
        Matchers.eq(60L),
        Mockito.eq(TimeUnit.SECONDS)
    );
    Assert.assertEquals(
        Long.valueOf(AbstractStatsCollectorTask.TELEMETRY_REPORT_PERIOD_SECONDS_DEFAULT).longValue(),
        task.getReportPeriodSeconds());
  }

  @Test
  public void testRollFrequencyTestMinutesConfiguration() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("abc");
    Mockito.when(buildInfo.getBuiltDate()).thenReturn(new Date().toString());
    Mockito.when(buildInfo.getBuiltBy()).thenReturn("System");

    String sdcId = "0123456789-0123456789-0123456789";
    RuntimeInfo runtimeInfo = mockRuntimeInfo(sdcId, testDir);

    Configuration config = new Configuration();
    config.set(AbstractStatsCollectorTask.TEST_ROLL_PERIOD_CONFIG, 20);

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);
    task.initTask();
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(20), task.getRollFrequencyMillis());

    //120 mins - will max at 60 mins
    config.set(AbstractStatsCollectorTask.TEST_ROLL_PERIOD_CONFIG, 120);
    scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, true);
    task.initTask();
    Assert.assertEquals(TimeUnit.HOURS.toMillis(AbstractStatsCollectorTask.ROLL_PERIOD_CONFIG_MAX), task.getRollFrequencyMillis());
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestStatsCollectorTask.class);

  public static final class UsageServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      boolean ok;
      String str = req.getContentType();
      if (str == null) {
        LOG.error("Missing content-type header");
        ok = false;
      } else {
        if (str.toLowerCase().startsWith("application/json")) {
          str = req.getHeader("x-requested-by");
          if (str == null) {
            LOG.error("Missing x-requested-by header");
            ok = false;
          } else {
            try {
              UUID.fromString(str);
              try {
                List<StatsBean> list = ObjectMapperFactory.get().readValue(
                    req.getReader(),
                    new TypeReference<List<StatsBean>>() {
                    }
                );
                if (list == null) {
                  LOG.error("Missing payload");
                  ok = false;
                } else {
                  if (list.isEmpty()) {
                    LOG.error("No stats in list");
                    ok = false;
                  } else {
                    ok = true;
                  }
                }
              } catch (IOException ex) {
                LOG.error("Invalid payload: " + ex);
                ok = false;
              }
            } catch (Exception ex) {
              LOG.error("Invalid x-requested-by header, should be SDC ID (a UUID): {}", ex, ex);
              ok = false;
            }
          }
        } else {
          LOG.error("Invalid content-type: {}", str);
          ok = false;
        }
      }
      resp.setStatus((ok) ? HttpServletResponse.SC_OK : HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  @Test
  public void testHttp() throws Exception {
    Server server = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    Servlet servlet = new UsageServlet();
    context.addServlet(new ServletHolder(servlet), AbstractStatsCollectorTask.USAGE_PATH_DEFAULT);
    context.setContextPath("/");
    server.setHandler(context);
    try {
      server.start();

      BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
      Mockito.when(buildInfo.getVersion()).thenReturn("v1");

      RuntimeInfo runtimeInfo = mockRuntimeInfo(UUID.randomUUID().toString(), null);

      Configuration config = new Configuration();

      AbstractStatsCollectorTask collector = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, null, true);

      List<StatsBean> list = Arrays.asList(new StatsBean());

      Assert.assertTrue(collector.reportStats(list));

    } finally {
      server.stop();
    }
  }

  private AbstractStatsCollectorTask mockStatsCollectorTask(
          BuildInfo buildInfo,
          RuntimeInfo runtimeInfo,
          Activation activation,
          Configuration config,
          SafeScheduledExecutorService executorService,
          boolean postTelemetrySuccess
  ) {
    SysInfo sysInfo = Mockito.mock(SysInfo.class);
    Mockito.when(sysInfo.toMap()).thenReturn(DEFAULT_SYS_INFO_MAP);
    AbstractStatsCollectorTask spy = Mockito.spy(new TestModelStatsCollectorTask(
            buildInfo, runtimeInfo, config, executorService, sysInfo, activation));

    // to test real interactions, comment out starting from here, change StatsCollectorTask.TELEMETRY_USE_TEST_BUCKET_DEFAULT to true, and run testReportStats
    // This will put a real file into the S3 bucket customer-support-bundles-test that you can verify.
    try {
      RestClient.Response getUrlResponse = Mockito.mock(RestClient.Response.class);
      Mockito.doReturn(getUrlResponse).when(spy).postToGetTelemetryUrl(Mockito.any(), Mockito.any());
      Mockito.when(getUrlResponse.successful()).thenReturn(postTelemetrySuccess);
      Mockito.when(getUrlResponse.getData(Mockito.any(TypeReference.class))).thenReturn(ImmutableMap.of(
          AbstractStatsCollectorTask.TELEMETRY_URL_KEY, POST_TELEMETRY_URL
      ));

      Mockito.doAnswer(new Answer() {
        @Override
        public HttpURLConnection answer(InvocationOnMock invocation) throws Throwable {
          uploadConnectionHolder[0] = Mockito.spy((HttpURLConnection) invocation.callRealMethod());
          // avoid real external calls
          ByteArrayOutputStream output = new ByteArrayOutputStream();
          Mockito.doReturn(output).when(uploadConnectionHolder[0]).getOutputStream();
          Mockito.doReturn(200).when(uploadConnectionHolder[0]).getResponseCode();
          return uploadConnectionHolder[0];
        }
      }).when(spy).getHttpURLConnection(Mockito.any());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // end section to comment out to perform real interactions

    return spy;
  }

  @Test
  public void testRunnableRollingNotPublishing() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, false);

    try (OutputStream os = new FileOutputStream(task.getOptFile())) {
      ObjectMapperFactory.get().writeValue(os, ImmutableMap.of(task.STATS_ACTIVE_KEY, true));
    }

    try (OutputStream os = new FileOutputStream(task.getStatsFile())) {
      StatsInfo statsInfo = new StatsInfo(task.provideStatsExtensions());
      statsInfo.getActiveStats().setSdcId("id");
      statsInfo.getActiveStats().setDataCollectorVersion("v1");
      statsInfo.getActiveStats().setBuildRepoSha("sha1");
      statsInfo.getActiveStats().setExtraInfo(ImmutableMap.of("a", "A"));
      ObjectMapperFactory.get().writeValue(os, statsInfo);
    }

    task.init();

    Assert.assertTrue(task.isOpted());
    Assert.assertTrue(task.isActive());

    //verifying we rolled the read stats
    Assert.assertEquals("v1", task.getStatsInfo().getActiveStats().getDataCollectorVersion());

    try (InputStream is = new FileInputStream(task.getStatsFile())) {
      StatsInfo statsInfo = ObjectMapperFactory.get().readValue(is, StatsInfo.class);
      Assert.assertEquals(DEFAULT_SYS_INFO_MAP, statsInfo.getExtraInfo(task.getSysInfo()));
      Assert.assertEquals(1, statsInfo.getCollectedStats().size());
      Assert.assertEquals("id",
          statsInfo.getCollectedStats().get(0).getSdcId());
      Assert.assertEquals("v1",
          statsInfo.getCollectedStats().get(0).getDataCollectorVersion());
      Assert.assertEquals("sha1",
          statsInfo.getCollectedStats().get(0).getBuildRepoSha());
      Assert.assertEquals(ImmutableMap.of("a", "A"),
          statsInfo.getCollectedStats().get(0).getExtraInfo());
    }
    task.stop();
  }

  @Test
  public void testSetActiveRunnableCalled() throws Exception {
    File testDir = createTestDir();

    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("v1");
    Mockito.when(buildInfo.getBuiltRepoSha()).thenReturn("sha1");

    RuntimeInfo runtimeInfo = mockRuntimeInfo("id", testDir);

    Configuration config = new Configuration();

    SafeScheduledExecutorService scheduler = Mockito.mock(SafeScheduledExecutorService.class);

    AbstractStatsCollectorTask task = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, scheduler, false);

    Map<Boolean, AtomicLong> runnableForceParamToTimes =
        new HashMap<>(ImmutableMap.of(Boolean.TRUE, new AtomicLong(0), Boolean.FALSE, new AtomicLong(0)));
    Mockito.when(task.getRunnable(Matchers.anyBoolean())).thenAnswer((Answer<Runnable>) invocation -> {
      final Runnable r = (Runnable) invocation.callRealMethod();
      return (Runnable) () -> {
        runnableForceParamToTimes.get(invocation.getArgumentAt(0, Boolean.class)).incrementAndGet();
        r.run();
      };
    });

    task.initTask();
    Assert.assertEquals(1, runnableForceParamToTimes.get(Boolean.TRUE).get());

    //Set InActive should not trigger roll and report
    task.setActive(false);
    // 1 from start
    Assert.assertEquals(1, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());


    //Set Active should trigger roll and report
    task.setActive(true);
    Assert.assertEquals(2, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());


    // try it again, but should not roll and report again
    task.setActive(true);
    Assert.assertEquals(2, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());

    //Stop should trigger roll and report
    task.stopTask();
    Assert.assertEquals(3, runnableForceParamToTimes.get(Boolean.TRUE).get());
    Assert.assertEquals(0, runnableForceParamToTimes.get(Boolean.FALSE).get());
  }

  private RuntimeInfo mockRuntimeInfo(String sdcId, File dataDir) {
    RuntimeInfo ret = Mockito.mock(RuntimeInfo.class);
    Mockito.when(ret.getId()).thenReturn(sdcId);
    Mockito.when(ret.getProductName()).thenReturn(RuntimeInfo.SDC_PRODUCT);
    if (dataDir != null) {
      Mockito.when(ret.getDataDir()).thenReturn(dataDir.getAbsolutePath());
    }
    Mockito.when(ret.getLibexecDir()).thenReturn(
        System.getenv("PWD").replace("container/src/main/.*","") + "/dist/src/main/libexec");
    return ret;
  }

  private AbstractStatsCollectorTask mockStatsCollectorTaskAndRunnable(
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      Configuration config,
      SafeScheduledExecutorService executorService) {

    AbstractStatsCollectorTask spy = mockStatsCollectorTask(buildInfo, runtimeInfo, null, config, executorService, true);

    runnable = Mockito.mock(Runnable.class);
    Mockito.doReturn(runnable).when(spy).getRunnable(Mockito.anyBoolean());

    return spy;
  }

  static class TestModelStatsCollectorTask extends AbstractStatsCollectorTask {
    public TestModelStatsCollectorTask(
        BuildInfo buildInfo,
        RuntimeInfo runtimeInfo,
        Configuration config,
        SafeScheduledExecutorService executorService,
        SysInfo sysInfo,
        Activation activation) {
      super(buildInfo, runtimeInfo, config, executorService, sysInfo, activation);
    }

    @Override
    protected List<AbstractStatsExtension> provideStatsExtensions() {
      return ImmutableList.of(
        new TestModelStatsExtension()
      );
    }
  }

}
