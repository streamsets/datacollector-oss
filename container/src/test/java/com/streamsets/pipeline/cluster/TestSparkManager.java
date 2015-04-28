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
import com.streamsets.pipeline.util.SystemProcess;
import com.streamsets.pipeline.util.SystemProcessFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;

public class TestSparkManager {

  private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

  private File tempDir;
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
    SystemProcessForTests.isAlive = false;
    SystemProcessForTests.output.clear();
    SystemProcessForTests.error.clear();
    pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(),
      null, new ArrayList<ConfigConfiguration>(), null, new ArrayList<StageConfiguration>(),
      MockStages.getErrorStageConfig());

  }

  @After
  public void tearDown() {
    tempDir.delete();
  }

  @Test
  public void testInvalidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher("");
    Assert.assertFalse(matcher.find());
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher("application_1429587312661_0024");
    Assert.assertFalse(matcher.find());
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher("_application_1429587312661_0024_");
    Assert.assertFalse(matcher.find());
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher(" pplication_1429587312661_0024 ");
    Assert.assertFalse(matcher.find());
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_00a24 ");
    Assert.assertFalse(matcher.find());
  }

  @Test
  public void testValidYARNAppId() throws Exception {
    Matcher matcher;
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher("15/04/21 21:15:20 INFO Client: Application report for application_1429587312661_0024 (state: RUNNING)");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher(" application_1429587312661_0024 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher("\tapplication_1429587312661_0024\t");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_1429587312661_0024", matcher.group(1));
    matcher = SparkManager.YARN_APPLICATION_ID_REGEX.
      matcher(" application_11111111111111111_9999924 ");
    Assert.assertTrue(matcher.find());
    Assert.assertEquals("application_11111111111111111_9999924", matcher.group(1));
  }

  @Test(expected = TimeoutException.class)
  public void testTimeOut() throws Throwable {
    File etcDir = new File(tempDir, "etc");
    Assert.assertTrue(etcDir.mkdir());
    File someFile = new File(etcDir, "somefile");
    Assert.assertTrue(someFile.createNewFile());
    URLClassLoader classLoader = new URLClassLoader(new URL[0]);
    StageLibraryTask stageLibrary = MockStages.createStageLibrary(classLoader);
    SparkManager sparkManager = new SparkManager(new SystemProcessFactoryForTests(), tempDir, sparkManagerShell,
      classLoader, classLoader);
    try {
      sparkManager.submit(pipelineConf, stageLibrary, etcDir, EMPTY_MAP, EMPTY_MAP, 1).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testMoreThanOneAppId() throws Throwable {
    SystemProcessForTests.output.add(" application_1429587312661_0024 ");
    SystemProcessForTests.output.add(" application_1429587312661_0025 ");
    File etcDir = new File(tempDir, "etc");
    Assert.assertTrue(etcDir.mkdir());
    File someFile = new File(etcDir, "somefile");
    Assert.assertTrue(someFile.createNewFile());
    URLClassLoader classLoader = new URLClassLoader(new URL[0]);
    StageLibraryTask stageLibrary = MockStages.createStageLibrary(classLoader);
    SparkManager sparkManager = new SparkManager(new SystemProcessFactoryForTests(), tempDir, sparkManagerShell,
      classLoader, classLoader);
    try {
      sparkManager.submit(pipelineConf, stageLibrary, etcDir, EMPTY_MAP, EMPTY_MAP, 1).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testSuccess() throws Throwable {
    String id = "application_1429587312661_0025";
    SystemProcessForTests.output.add(" " + id + " ");
    SystemProcessForTests.output.add(" " + id + " ");
    File etcDir = new File(tempDir, "etc");
    Assert.assertTrue(etcDir.mkdir());
    File someFile = new File(etcDir, "somefile");
    Assert.assertTrue(someFile.createNewFile());
    URLClassLoader classLoader = new URLClassLoader(new URL[0]);
    StageLibraryTask stageLibrary = MockStages.createStageLibrary(classLoader);
    SparkManager sparkManager = new SparkManager(new SystemProcessFactoryForTests(), tempDir, sparkManagerShell,
      classLoader, classLoader);
    sparkManager.submit(pipelineConf, stageLibrary, etcDir, EMPTY_MAP, EMPTY_MAP, 1).get();
  }

  private static class SystemProcessForTests extends SystemProcess {
    static boolean isAlive = false;
    static final List<String> output = new ArrayList<>();
    static final List<String> error = new ArrayList<>();

    public SystemProcessForTests(File tempDir) {
      super("dummy", tempDir, new ArrayList<String>());
    }

    @Override
    public void start() throws IOException {
      // do nothing
    }

    @Override
    public boolean isAlive() {
      return isAlive;
    }

    @Override
    public List<String> getAllOutput() {
      return output;
    }
    @Override
    public List<String> getAllError() {
      return error;
    }

    @Override
    public List<String> getOutput() {
      return output;
    }

    @Override
    public List<String> getError() {
      return error;
    }

    @Override
    public String getCommand() {
      return "";
    }

    @Override
    public void cleanup() {
      // do nothing
    }

    @Override
    public int exitValue() {
      return 0;
    }

    @Override
    public void kill(long timeoutBeforeForceKill) {

    }

    @Override
    public boolean waitFor(long timeout, TimeUnit unit)
      throws InterruptedException {
      return true;
    }
  }
  private static class SystemProcessFactoryForTests extends SystemProcessFactory {
    public SystemProcess create(String name, File tempDir, List<String> args) {
      return new SystemProcessForTests(tempDir);
    }
  }
}
