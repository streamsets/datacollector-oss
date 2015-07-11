/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.creation.PipelineConfigBean;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestClusterProviderImpl {

  private File tempDir;
  private File providerTemp;
  private File etcDir;
  private File resourcesDir;
  private File webDir;
  private File bootstrapLibDir;
  private PipelineConfiguration pipelineConf;
  private File sparkManagerShell;
  private URLClassLoader classLoader;
  private StageLibraryTask stageLibrary;
  private Map<String, String> env;
  private Map<String, String> sourceInfo;
  private ClusterProviderImpl sparkProvider;


  @Before
  public void setup() throws Exception {
    tempDir = File.createTempFile(getClass().getSimpleName(), "");
    sparkManagerShell = new File(tempDir, "_cluster-manager");
    Assert.assertTrue(tempDir.delete());
    Assert.assertTrue(tempDir.mkdir());
    providerTemp = new File(tempDir, "provider-temp");
    Assert.assertTrue(providerTemp.mkdir());
    Assert.assertTrue(sparkManagerShell.createNewFile());
    sparkManagerShell.setExecutable(true);
    MockSystemProcess.reset();
    etcDir = new File(tempDir, "etc-src");
    Assert.assertTrue(etcDir.mkdir());
    File sdcProperties = new File(etcDir, "sdc.properties");
    Assert.assertTrue(sdcProperties.createNewFile());
    resourcesDir = new File(tempDir, "resources-src");
    Assert.assertTrue(resourcesDir.mkdir());
    Assert.assertTrue((new File(resourcesDir, "dir")).mkdir());
    Assert.assertTrue((new File(resourcesDir, "file")).createNewFile());
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
    List<ConfigConfiguration> configs = new ArrayList<ConfigConfiguration>();
    configs.add(new ConfigConfiguration("clusterSlaveMemory", 512));
    configs.add(new ConfigConfiguration("clusterSlaveJavaOpts", ""));
    configs.add(new ConfigConfiguration("clusterKerberos", false));
    configs.add(new ConfigConfiguration("kerberosPrincipal", ""));
    configs.add(new ConfigConfiguration("kerberosKeytab", ""));
    pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION, UUID.randomUUID(),
      null, configs, null, new ArrayList<StageConfiguration>(),
      MockStages.getErrorStageConfig());
    File sparkKafkaJar = new File(tempDir, ClusterModeConstants.SPARK_KAFKA_JAR_PREFIX + ".jar");
    Assert.assertTrue(sparkKafkaJar.createNewFile());
    classLoader = new URLClassLoader(new URL[] {sparkKafkaJar.toURL()}) {
      public String getType() {
        return ClusterModeConstants.USER_LIBS;
      }
    };
    stageLibrary = MockStages.createStageLibrary(classLoader);
    env = new HashMap<>();
    sourceInfo = new HashMap<>();
    sourceInfo.put(ClusterModeConstants.NUM_EXECUTORS_KEY, "64");
    sourceInfo.put(ClusterModeConstants.CLUSTER_SOURCE_NAME, "kafka");
    sparkProvider = new ClusterProviderImpl();
  }

  @After
  public void tearDown() {
    FileUtils.deleteQuietly(tempDir);
  }

  @Test(expected = IllegalStateException.class)
  public void testMoreThanOneAppId() throws Throwable {
    MockSystemProcess.output.add(" application_1429587312661_0024 ");
    MockSystemProcess.output.add(" application_1429587312661_0025 ");
    Assert.assertNotNull(sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, stageLibrary, etcDir, resourcesDir, webDir,
      bootstrapLibDir, classLoader, classLoader,  60).getId());
  }

  @Test
  public void testSuccess() throws Throwable {
    String id = "application_1429587312661_0025";
    MockSystemProcess.output.add(" " + id + " ");
    MockSystemProcess.output.add(" " + id + " ");
    Assert.assertEquals(id, sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, stageLibrary, etcDir, resourcesDir, webDir,
      bootstrapLibDir, classLoader, classLoader, 60).getId());
    Assert.assertEquals(Arrays.asList("<masked>/_cluster-manager", "start", "--master", "yarn-cluster",
      "--executor-memory", "512m", "--executor-cores", "1", "--num-executors", "64", "--archives",
      "<masked>/provider-temp/libs.tar.gz,<masked>/provider-temp/etc.tar.gz,<masked>/provider-temp/resources.tar.gz",
      "--files", "<masked>/provider-temp/log4j.properties", "--jars",
      "<masked>/bootstrap-lib/main/streamsets-datacollector-bootstrap.jar,<masked>/spark-streaming-kafka.jar",
      "--conf", "spark.executor.extraJavaOptions=-javaagent:./streamsets-datacollector-bootstrap.jar " , "--class",
      "com.streamsets.pipeline.BootstrapCluster",
      "<masked>/bootstrap-lib/spark/streamsets-datacollector-spark-bootstrap.jar"), MockSystemProcess.args);
  }
}
