/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.runner.MockStages;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;

public class TestSparkManager {

  private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

  private File tempDir;
  private File etcDir;
  private File webDir;
  private File bootstrapLibDir;
  private PipelineConfiguration pipelineConf;
  private File sparkManagerShell;

  @Before
  public void setup() throws Exception {
    tempDir = File.createTempFile(getClass().getSimpleName(), "");
    sparkManagerShell = new File(tempDir, "spark-manager");
    Assert.assertTrue(tempDir.delete());
    Assert.assertTrue(tempDir.mkdir());
    Assert.assertTrue(sparkManagerShell.createNewFile());
    sparkManagerShell.setExecutable(true);
    MockSystemProcess.isAlive = false;
    MockSystemProcess.output.clear();
    MockSystemProcess.error.clear();
    etcDir = new File(tempDir, "etc-src");
    Assert.assertTrue(etcDir.mkdir());
    File sdcProperties = new File(etcDir, "sdc.properties");
    Assert.assertTrue(sdcProperties.createNewFile());
    webDir = new File(tempDir, "static-web-dir-src");
    Assert.assertTrue(webDir.mkdir());
    File someWebFile = new File(webDir, "somefile");
    Assert.assertTrue(someWebFile.createNewFile());
    bootstrapLibDir = new File(tempDir, "bootstrap-lib");
    Assert.assertTrue(bootstrapLibDir.mkdir());
    File bootstrapMainLibDir = new File(bootstrapLibDir, "main");
    Assert.assertTrue(bootstrapMainLibDir.mkdirs());
    File bootstrapSparkLibDir = new File(bootstrapLibDir, "spark");
    Assert.assertTrue(bootstrapSparkLibDir.mkdirs());
    Assert.assertTrue(new File(bootstrapMainLibDir, "streamsets-datacollector-bootstrap.jar").createNewFile());
    Assert.assertTrue(new File(bootstrapSparkLibDir, "streamsets-datacollector-spark-bootstrap.jar").createNewFile());
    pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(),
      null, new ArrayList<ConfigConfiguration>(), null, new ArrayList<StageConfiguration>(),
      MockStages.getErrorStageConfig());
  }

  @After
  public void tearDown() {
    FileUtils.deleteQuietly(tempDir);
  }

  @Test
  public void testInvalidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("");
    Assert.assertFalse(matcher.find());
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("application_1429587312661_0024");
    Assert.assertFalse(matcher.find());
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("_application_1429587312661_0024_");
    Assert.assertFalse(matcher.find());
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" pplication_1429587312661_0024 ");
    Assert.assertFalse(matcher.find());
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_00a24 ");
    Assert.assertFalse(matcher.find());
  }

  @Test
  public void testValidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("15/04/21 21:15:20 INFO Client: Application report for application_1429587312661_0024 (state: RUNNING)");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_0024 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher("\tapplication_1429587312661_0024\t");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = SparkProviderImpl.YARN_APPLICATION_ID_REGEX.
      matcher(" application_11111111111111111_9999924 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_11111111111111111_9999924", matcher.group(1));
  }

  @Test(expected = TimeoutException.class)
  public void testTimeOut() throws Throwable {
    URLClassLoader classLoader = new URLClassLoader(new URL[0]);
    StageLibraryTask stageLibrary = MockStages.createStageLibrary(classLoader);
    MockSparkProvider sparkProvider = new MockSparkProvider();
    sparkProvider.submitTimesOut = true;
    SparkManager sparkManager = new SparkManager(new MockSystemProcessFactory(), sparkProvider, tempDir,
      sparkManagerShell, classLoader, classLoader, 1);
    try {
      sparkManager.submit(pipelineConf, stageLibrary, etcDir, webDir, bootstrapLibDir, EMPTY_MAP, EMPTY_MAP).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testMoreThanOneAppId() throws Throwable {
    MockSystemProcess.output.add(" application_1429587312661_0024 ");
    MockSystemProcess.output.add(" application_1429587312661_0025 ");
    URLClassLoader classLoader = new URLClassLoader(new URL[0]);
    StageLibraryTask stageLibrary = MockStages.createStageLibrary(classLoader);
    SparkManager sparkManager = new SparkManager(new MockSystemProcessFactory(), new SparkProviderImpl(), tempDir,
      sparkManagerShell, classLoader, classLoader, 1);
    try {
      sparkManager.submit(pipelineConf, stageLibrary, etcDir, webDir, bootstrapLibDir, EMPTY_MAP, EMPTY_MAP).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testSuccess() throws Throwable {
    String id = "application_1429587312661_0025";
    MockSystemProcess.output.add(" " + id + " ");
    MockSystemProcess.output.add(" " + id + " ");
    URLClassLoader classLoader = new URLClassLoader(new URL[0]);
    StageLibraryTask stageLibrary = MockStages.createStageLibrary(classLoader);
    SparkManager sparkManager = new SparkManager(new MockSystemProcessFactory(), new SparkProviderImpl(), tempDir,
      sparkManagerShell, classLoader, classLoader, 1);
    sparkManager.submit(pipelineConf, stageLibrary, etcDir, webDir, bootstrapLibDir, EMPTY_MAP, EMPTY_MAP).get();
  }
}
