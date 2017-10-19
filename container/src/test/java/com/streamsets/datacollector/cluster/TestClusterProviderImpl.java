/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.cluster;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.StandaloneRuntimeInfo;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class TestClusterProviderImpl {

  @Rule
  public TemporaryFolder tempFolder= new TemporaryFolder();
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
  private final String SDC_TEST_PREFIX = "dummy";


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
    File log4jPropertyDummyFile = new File(etcDir, SDC_TEST_PREFIX + RuntimeInfo.LOG4J_PROPERTIES);
    Assert.assertTrue(log4jPropertyDummyFile.createNewFile());
    resourcesDir = new File(tempDir, "resources-src");
    Assert.assertTrue(resourcesDir.mkdir());
    Assert.assertTrue((new File(resourcesDir, "dir")).mkdir());
    File resourcesSubDir = new File(resourcesDir, "dir");
    File resourceFile = new File(resourcesSubDir, "core-site.xml");
    resourceFile.createNewFile();
    Assert.assertTrue((new File(resourcesDir, "file")).createNewFile());
    webDir = new File(tempDir, "static-web-dir-src");
    Assert.assertTrue(webDir.mkdir());
    File someWebFile = new File(webDir, "somefile");
    Assert.assertTrue(someWebFile.createNewFile());
    bootstrapLibDir = new File(tempDir, "bootstrap-lib");
    Assert.assertTrue(bootstrapLibDir.mkdir());
    File bootstrapMainLibDir = new File(bootstrapLibDir, "main");
    Assert.assertTrue(bootstrapMainLibDir.mkdirs());
    File bootstrapClusterLibDir = new File(bootstrapLibDir, "cluster");
    Assert.assertTrue(bootstrapClusterLibDir.mkdirs());
    Assert.assertTrue(new File(bootstrapMainLibDir, "streamsets-datacollector-bootstrap-1.7.0.0-SNAPSHOT.jar")
        .createNewFile());
    Assert.assertTrue(new File(bootstrapClusterLibDir,
        "streamsets-datacollector-cluster-bootstrap-1.7.0.0-SNAPSHOT.jar"
    ).createNewFile());
    Assert.assertTrue(new File(bootstrapClusterLibDir,
        "streamsets-datacollector-cluster-bootstrap-api-1.7.0.0-SNAPSHOT.jar"
    ).createNewFile());
    Assert.assertTrue(new File(bootstrapClusterLibDir, "streamsets-datacollector-mesos-bootstrap-1.7.0.0.jar")
        .createNewFile());
    Assert.assertTrue(new File(bootstrapClusterLibDir,
        "streamsets-datacollector-mapr-cluster-bootstrap-1.7.0.0.jar"
    ).createNewFile());
    List<Config> configs = new ArrayList<>();
    configs.add(new Config("clusterSlaveMemory", 512));
    configs.add(new Config("clusterSlaveJavaOpts", ""));
    configs.add(new Config("clusterKerberos", false));
    configs.add(new Config("kerberosPrincipal", ""));
    configs.add(new Config("kerberosKeytab", ""));
    configs.add(new Config("executionMode", ExecutionMode.CLUSTER_YARN_STREAMING));
    configs.add(new Config("sparkConfigs", Arrays.asList(new HashMap<String, String>() {{
      put("key", "a");
      put("value", "b");
    }})));
    pipelineConf = new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        configs,
        null,
        ImmutableList.of(MockStages.createSource("s", ImmutableList.of("S"))),
        MockStages.getErrorStageConfig(),
        MockStages.getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    pipelineConf.setPipelineInfo(new PipelineInfo("name", "label", "desc", null, null,
      "aaa", null, null, null, true, null, null, "x"));
    File sparkKafkaJar = new File(tempDir, "spark-streaming-kafka-1.2.jar");
    File avroJar = new File(tempDir, "avro-1.7.7.jar");
    File avroMapReduceJar = new File(tempDir, "avro-mapred-1.7.7.jar");
    File maprFsJar = new File(tempDir, "maprfs-5.1.0.jar");
    Assert.assertTrue(sparkKafkaJar.createNewFile());
    Assert.assertTrue(avroJar.createNewFile());
    Assert.assertTrue(avroMapReduceJar.createNewFile());
    Assert.assertTrue(maprFsJar.createNewFile());
    classLoader = new URLClassLoader(new URL[]{sparkKafkaJar.toURI().toURL(), avroJar.toURI().toURL(), avroMapReduceJar.toURI().toURL(),
        maprFsJar.toURI().toURL()}) {
      public String getType() {
        return ClusterModeConstants.USER_LIBS;
      }
    };
    stageLibrary = MockStages.createStageLibrary(classLoader);
    env = new HashMap<>();
    sourceInfo = new HashMap<>();
    sourceInfo.put(ClusterModeConstants.NUM_EXECUTORS_KEY, "64");
    URLClassLoader emptyCL = new URLClassLoader(new URL[0]);
    RuntimeInfo runtimeInfo = new StandaloneRuntimeInfo(SDC_TEST_PREFIX, null, Arrays.asList(emptyCL), tempDir);
    sparkProvider = Mockito.spy(new ClusterProviderImpl(runtimeInfo, null, null));
    Mockito.doReturn(ClusterProviderImpl.CLUSTER_BOOTSTRAP_API_JAR_PATTERN).when(sparkProvider).findClusterBootstrapJar(
        Mockito.eq(ExecutionMode.CLUSTER_BATCH),
        Mockito.any(PipelineConfiguration.class),
        Mockito.any(StageLibraryTask.class)
    );
    Mockito.doReturn(ClusterProviderImpl.CLUSTER_BOOTSTRAP_JAR_PATTERN).when(sparkProvider).findClusterBootstrapJar(
        Mockito.eq(ExecutionMode.CLUSTER_YARN_STREAMING),
        Mockito.any(PipelineConfiguration.class),
        Mockito.any(StageLibraryTask.class)
    );
    Mockito.doReturn(ClusterProviderImpl.CLUSTER_BOOTSTRAP_MESOS_JAR_PATTERN).when(sparkProvider)
        .findClusterBootstrapJar(
        Mockito.eq(ExecutionMode.CLUSTER_MESOS_STREAMING),
        Mockito.any(PipelineConfiguration.class),
        Mockito.any(StageLibraryTask.class)
    );
  }

  @After
  public void tearDown() {
    FileUtils.deleteQuietly(tempDir);
  }

  @Test
  public void testCopyDirectory() throws Exception {
    File copyTempDir = new File(tempDir, "copy");
    File srcDir = new File(copyTempDir, "somedir");
    File dstDir = new File(copyTempDir, "dst");
    Assert.assertTrue(srcDir.mkdirs());
    Assert.assertTrue(dstDir.mkdirs());
    File link1 = new File(copyTempDir, "link1");
    File link2 = new File(copyTempDir, "link2");
    File dir1 = new File(copyTempDir, "dir1");
    File file1 = new File(dir1, "f1");
    File file2 = new File(dir1, "f2");
    Assert.assertTrue(dir1.mkdirs());
    Assert.assertTrue(file1.createNewFile());
    Assert.assertTrue(file2.createNewFile());
    file2.setReadable(false);
    file2.setWritable(false);
    file2.setExecutable(false);
    Files.createSymbolicLink(link1.toPath(), dir1.toPath());
    Files.createSymbolicLink(link2.toPath(), link1.toPath());
    Files.createSymbolicLink(new File(srcDir, "dir1").toPath(), link2.toPath());
    File clone = ClusterProviderImpl.createDirectoryClone(srcDir, srcDir.getName(), dstDir);
    File cloneF1 = new File(new File(clone, "dir1"), "f1");
    Assert.assertTrue(cloneF1.isFile());
  }

  @Test(expected = IllegalStateException.class)
  public void testMoreThanOneAppId() throws Throwable {
    MockSystemProcess.output.add(" application_1429587312661_0024 ");
    MockSystemProcess.output.add(" application_1429587312661_0025 ");
    Assert.assertNotNull(sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, stageLibrary, etcDir, resourcesDir, webDir,
      bootstrapLibDir, classLoader, classLoader,  60,
        new RuleDefinitions(
            PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
            RuleDefinitionsConfigBean.VERSION,
            new ArrayList<MetricsRuleDefinition>(),
            new ArrayList<DataRuleDefinition>(),
            new ArrayList<DriftRuleDefinition>(),
            new ArrayList<String>(),
            UUID.randomUUID(),
            Collections.emptyList()
        ), null).getId());
  }

  @Test
  public void testYarnStreamingExecutionMode() throws Throwable {
    MockSystemProcess.output.add(" application_1429587312661_0024 ");
    List<Config> list = new ArrayList<Config>();
    list.add(new Config("executionMode", ExecutionMode.CLUSTER_YARN_STREAMING.name()));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        list,
        null,
        MockStages.getSourceStageConfig(),
        MockStages.getErrorStageConfig(),
        MockStages.getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    pipelineConf.setPipelineInfo(new PipelineInfo("name", "desc", "label", null, null,
      "aaa", null, null, null, true, null, "2.6", "x"));
    Assert.assertNotNull(sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, MockStages.createClusterStreamingStageLibrary(classLoader), etcDir, resourcesDir,
      webDir, bootstrapLibDir, classLoader, classLoader,  60,
      new RuleDefinitions(
          PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
          RuleDefinitionsConfigBean.VERSION,
          Collections.<MetricsRuleDefinition>emptyList(),
          Collections.<DataRuleDefinition>emptyList(),
          Collections.<DriftRuleDefinition>emptyList(),
          Collections.<String>emptyList(),
          UUID.randomUUID(),
          Collections.<Config>emptyList()
      ), null).getId());
    Assert.assertEquals(ClusterProviderImpl.CLUSTER_TYPE_YARN,
      MockSystemProcess.env.get(ClusterProviderImpl.CLUSTER_TYPE));
    Assert.assertTrue(MockSystemProcess.args.contains(
        "<masked>/bootstrap-lib/main/streamsets-datacollector-bootstrap-1.7.0.0-SNAPSHOT.jar," +
            "<masked>/spark-streaming-kafka-1.2" +
            ".jar,<masked>/bootstrap-lib/cluster/streamsets-datacollector-cluster-bootstrap-api-1.7.0.0-SNAPSHOT.jar"));
  }

  @Test
  public void testMesosStreamingExecutionMode() throws Throwable {
    MockSystemProcess.output.add(" driver-20151105162031-0005 ");
    List<Config> list = new ArrayList<Config>();
    list.add(new Config("executionMode", ExecutionMode.CLUSTER_MESOS_STREAMING.name()));
    list.add(new Config("hdfsS3ConfDir", "dir"));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        list,
        null,
        MockStages.getSourceStageConfig(),
        MockStages.getErrorStageConfig(),
        MockStages.getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    pipelineConf.setPipelineInfo(new PipelineInfo("name", "desc", "label", null, null,
      "aaa", null, null, null, true, null, "2.6", "x"));
    ApplicationState appState = sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, MockStages.createClusterStreamingStageLibrary(classLoader), etcDir, resourcesDir,
      webDir, bootstrapLibDir, classLoader, classLoader,  60,
      new RuleDefinitions(
          PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
          RuleDefinitionsConfigBean.VERSION,
          Collections.<MetricsRuleDefinition>emptyList(),
          Collections.<DataRuleDefinition>emptyList(),
          Collections.<DriftRuleDefinition>emptyList(),
          Collections.<String>emptyList(),
          UUID.randomUUID(),
          Collections.<Config>emptyList()
      ), null);
    Assert.assertNotNull(appState.getId());
    Assert.assertNotNull(appState.getDirId());
    Assert.assertEquals(ClusterProviderImpl.CLUSTER_TYPE_MESOS,
      MockSystemProcess.env.get(ClusterProviderImpl.CLUSTER_TYPE));
    Assert.assertTrue(MockSystemProcess.args.contains(RuntimeInfo.UNDEF + "/" + appState.getDirId().get() +
        "/streamsets-datacollector-mesos-bootstrap-1.7.0.0.jar"));
  }

  @Test
  public void testMapRStreamingMode() throws Exception {
    MockSystemProcess.output.add(" application_1429587312661_0024 ");
    List<Config> list = new ArrayList<Config>();
    list.add(new Config("executionMode", ExecutionMode.CLUSTER_YARN_STREAMING.name()));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        list,
        null,
        MockStages.getSourceStageConfig(),
        MockStages.getErrorStageConfig(),
        MockStages.getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    pipelineConf.setPipelineInfo(new PipelineInfo("name", "label", "desc", null, null, "aaa", null, null, null, true, null, "2.6", null));
    Mockito.doReturn(Pattern.compile("streamsets-datacollector-mapr-cluster-bootstrap-\\d+.*")).when(sparkProvider)
        .findClusterBootstrapJar(
        Mockito.eq(ExecutionMode.CLUSTER_YARN_STREAMING),
        Mockito.any(PipelineConfiguration.class),
        Mockito.any(StageLibraryTask.class)
    );
    Assert.assertNotNull(sparkProvider.startPipeline(new MockSystemProcessFactory(),
        sparkManagerShell,
        providerTemp,
        env,
        sourceInfo,
        pipelineConf,
        MockStages.createClusterMapRStreamingStageLibrary(classLoader),
        etcDir,
        resourcesDir,
        webDir,
        bootstrapLibDir,
        classLoader,
        classLoader,
        60,
        new RuleDefinitions(
            PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
            RuleDefinitionsConfigBean.VERSION,
            Collections.<MetricsRuleDefinition>emptyList(),
            Collections.<DataRuleDefinition>emptyList(),
            Collections.<DriftRuleDefinition>emptyList(),
            Collections.<String>emptyList(),
            UUID.randomUUID(),
            Collections.<Config>emptyList()
        ),
        null
    ).getId());
    Assert.assertEquals(ClusterProviderImpl.CLUSTER_TYPE_YARN,
        MockSystemProcess.env.get(ClusterProviderImpl.CLUSTER_TYPE)
    );
    Assert.assertTrue(MockSystemProcess.args.contains(
        "<masked>/bootstrap-lib/main/streamsets-datacollector-bootstrap-1.7.0.0-SNAPSHOT.jar," + "<masked>/maprfs-5.1" +
            ".0.jar," +
            "<masked>/bootstrap-lib/cluster/streamsets-datacollector-cluster-bootstrap-api-1.7.0.0-SNAPSHOT.jar"));
    Assert.assertTrue(MockSystemProcess.args.contains(
        "<masked>/bootstrap-lib/cluster/streamsets-datacollector-mapr-cluster-bootstrap-1.7.0.0.jar"));
  }

  @Test
  public void testClusterBoostrapRegex() throws Exception {
    Properties props = ClusterProviderImpl.readDataCollectorProperties(Thread.currentThread().getContextClassLoader());
    Assert.assertEquals("abc", props.getProperty(ClusterProviderImpl.CLUSTER_BOOTSTRAP_JAR_REGEX +
        ExecutionMode.CLUSTER_YARN_STREAMING +
        "_Foo"));
  }

  @Test
  public void testBatchExecutionMode() throws Throwable {
    MockSystemProcess.output.add(" application_1429587312661_0024 ");
    List<Config> list = new ArrayList<Config>();
    list.add(new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()));
    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        PipelineStoreTask.SCHEMA_VERSION,
        PipelineConfigBean.VERSION,
        "pipelineId",
        UUID.randomUUID(),
        "label",
        null,
        list,
        null,
        MockStages.getSourceStageConfig(),
        MockStages.getErrorStageConfig(),
        MockStages.getStatsAggregatorStageConfig(),
        Collections.emptyList(),
        Collections.emptyList()
    );
    pipelineConf.setPipelineInfo(new PipelineInfo("name", "label", "desc", null, null,
      "aaa", null, null, null, true, null, "x", "y"));
    Assert.assertNotNull(sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, MockStages.createClusterBatchStageLibrary(classLoader), etcDir, resourcesDir, webDir,
      bootstrapLibDir, classLoader, classLoader,  60,
        new RuleDefinitions(
            PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
            RuleDefinitionsConfigBean.VERSION,
            Collections.<MetricsRuleDefinition>emptyList(),
            Collections.<DataRuleDefinition>emptyList(),
            Collections.<DriftRuleDefinition>emptyList(),
            Collections.<String>emptyList(),
            UUID.randomUUID(),
            Collections.<Config>emptyList()
        ), null).getId());
    Assert.assertEquals(ClusterProviderImpl.CLUSTER_TYPE_MAPREDUCE, MockSystemProcess.env.get(ClusterProviderImpl.CLUSTER_TYPE));
    Assert.assertTrue(MockSystemProcess.args.contains(
        "<masked>/bootstrap-lib/main/streamsets-datacollector-bootstrap-1.7.0.0-SNAPSHOT.jar," + "<masked>/avro-1.7.7" +
            ".jar," + "<masked>/avro-mapred-1.7.7.jar"
            ));
    Assert.assertTrue(MockSystemProcess.args.contains(
       "<masked>/bootstrap-lib/cluster/streamsets-datacollector-cluster-bootstrap-api-1.7.0.0-SNAPSHOT.jar"
    ));
  }

  @Test
  public void testKerberosError() throws Throwable {
    MockSystemProcess.output.add("Caused by: javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)]");
    MockSystemProcess.output.add("\tat com.sun.security.sasl.gsskerb.GssKrb5Client.evaluateChallenge(GssKrb5Client.java:212)");
    try {
      sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, stageLibrary, etcDir, resourcesDir, webDir,
      bootstrapLibDir, classLoader, classLoader,  60,
          new RuleDefinitions(
              PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
              RuleDefinitionsConfigBean.VERSION,
              Collections.<MetricsRuleDefinition>emptyList(),
              Collections.<DataRuleDefinition>emptyList(),
              Collections.<DriftRuleDefinition>emptyList(),
              Collections.<String>emptyList(),
              UUID.randomUUID(),
              Collections.<Config>emptyList()
          ), null).getId();
      Assert.fail("Expected IO Exception");
    } catch (IOException ex) {
      Assert.assertTrue("Incorrect message: " + ex, ex.getMessage().contains("No valid credentials provided"));
    }
  }

  @Test
  public void testSuccess() throws Throwable {
    String id = "application_1429587312661_0025";
    MockSystemProcess.output.add(" " + id + " ");
    MockSystemProcess.output.add(" " + id + " ");
    Assert.assertEquals(id, sparkProvider.startPipeline(new MockSystemProcessFactory(), sparkManagerShell,
      providerTemp, env, sourceInfo, pipelineConf, stageLibrary, etcDir, resourcesDir, webDir,
      bootstrapLibDir, classLoader, classLoader, 60, new RuleDefinitions(
            PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
            RuleDefinitionsConfigBean.VERSION,
            Collections.<MetricsRuleDefinition>emptyList(),
            Collections.<DataRuleDefinition>emptyList(),
            Collections.<DriftRuleDefinition>emptyList(),
            Collections.<String>emptyList(),
            UUID.randomUUID(),
            Collections.<Config>emptyList()
        ), null).getId());
      Assert.assertArrayEquals(
        new String[]{"<masked>/_cluster-manager", "start", "--master", "yarn", "--deploy-mode", "cluster", "--executor-memory", "512m",
            "--executor-cores", "1", "--num-executors", "64", "--archives", "<masked>/provider-temp/staging/libs.tar" +
            ".gz,<masked>/provider-temp/staging/etc.tar.gz,<masked>/provider-temp/staging/resources.tar.gz",
            "--files", "<masked>/provider-temp/staging/log4j.properties", "--jars",
            "<masked>/bootstrap-lib/main/streamsets-datacollector-bootstrap-1.7.0.0-SNAPSHOT.jar," +
                "<masked>/bootstrap-lib/cluster/streamsets-datacollector-cluster-bootstrap-api-1.7.0.0-SNAPSHOT.jar",
            "--conf", "spark" +
            ".executor.extraJavaOptions=-javaagent:./streamsets-datacollector-bootstrap-1.7.0.0-SNAPSHOT.jar ",
            "--conf", "a=b",
            "--name", "StreamSets Data Collector: label",
            "--class", "com" +
            ".streamsets.pipeline.BootstrapClusterStreaming",
            "<masked>/bootstrap-lib/cluster/streamsets-datacollector-cluster-bootstrap-1.7.0.0-SNAPSHOT.jar"},
        MockSystemProcess.args.toArray()
    );
  }

  @Test
  public void testCopyDpmToken() throws Exception {
    Properties sdcProperties = new Properties();
    sdcProperties.put(Configuration.CONFIG_INCLUDES, "dpm-test.properties");
    File etcDir = tempFolder.newFolder();
    Properties dpmProperties = new Properties();
    dpmProperties.setProperty("foo", "fooVal");
    dpmProperties.setProperty(RemoteSSOService.DPM_ENABLED, "true");
    File appTokenFile = tempFolder.newFile();
    try (PrintWriter out = new PrintWriter(appTokenFile)) {
      out.println("app-token-dummy-text");
    }
    // dpm enabled and app token is absolute
    dpmProperties.setProperty(
        RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG,
        Configuration.FileRef.DELIMITER + appTokenFile.getAbsolutePath() + Configuration.FileRef.DELIMITER
    );

    try (OutputStream out = new FileOutputStream(new File(etcDir, "dpm-test.properties"))) {
      dpmProperties.store(out, null);
    }
    sparkProvider.copyDpmTokenIfRequired(sdcProperties, etcDir);
    try (InputStream in = new FileInputStream(new File(etcDir, "dpm-test.properties"))) {
      Properties gotProperties = new Properties();
      gotProperties.load(in);
      Assert.assertEquals("fooVal", gotProperties.getProperty("foo"));
      Assert.assertEquals("true", gotProperties.getProperty(RemoteSSOService.DPM_ENABLED));
      Assert.assertEquals(
          Configuration.FileRef.DELIMITER + ClusterProviderImpl.CLUSTER_DPM_APP_TOKEN + Configuration.FileRef.DELIMITER,
          gotProperties.getProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG)
      );
      List<String> gotLines = Files.readAllLines(new File(etcDir, ClusterProviderImpl.CLUSTER_DPM_APP_TOKEN).toPath(),
          Charset.defaultCharset()
      );
      Assert.assertEquals(1, gotLines.size());
      Assert.assertEquals("app-token-dummy-text", gotLines.get(0));
    }
    // dpm not enabled
    etcDir = tempFolder.newFolder();
    dpmProperties = new Properties();
    dpmProperties.setProperty("foo", "fooNewVal");
    dpmProperties.setProperty(RemoteSSOService.DPM_ENABLED, "false");
    try (OutputStream out = new FileOutputStream(new File(etcDir, "dpm-test.properties"))) {
      dpmProperties.store(out, null);
    }
    sparkProvider.copyDpmTokenIfRequired(sdcProperties, etcDir);
    try (InputStream in = new FileInputStream(new File(etcDir, "dpm-test.properties"))) {
      Properties gotProperties = new Properties();
      gotProperties.load(in);
      Assert.assertEquals("fooNewVal", gotProperties.getProperty("foo"));
      Assert.assertEquals("false", gotProperties.getProperty(RemoteSSOService.DPM_ENABLED));
    }
    // dpm enabled but token relative
    etcDir = tempFolder.newFolder();
    dpmProperties = new Properties();
    dpmProperties.setProperty("foo", "fooDpmEnabledTokenRelative");
    dpmProperties.setProperty(RemoteSSOService.DPM_ENABLED, "true");
    dpmProperties.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG, Configuration.FileRef.DELIMITER +
        "relative_path_to_token.txt" + Configuration.FileRef.DELIMITER);
    try (OutputStream out = new FileOutputStream(new File(etcDir, "dpm-test.properties"))) {
      dpmProperties.store(out, null);
    }
    sparkProvider.copyDpmTokenIfRequired(sdcProperties, etcDir);
    try (InputStream in = new FileInputStream(new File(etcDir, "dpm-test.properties"))) {
      Properties gotProperties = new Properties();
      gotProperties.load(in);
      Assert.assertEquals("fooDpmEnabledTokenRelative", gotProperties.getProperty("foo"));
      Assert.assertEquals("true", gotProperties.getProperty(RemoteSSOService.DPM_ENABLED));
      Assert.assertEquals(Configuration.FileRef.DELIMITER +
              "relative_path_to_token.txt" + Configuration.FileRef.DELIMITER,
          gotProperties.getProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG)
      );
    }
    // all configs in sdc.properties (similar to parcels)
    sdcProperties.remove(Configuration.CONFIG_INCLUDES);
    sdcProperties.setProperty(RemoteSSOService.DPM_ENABLED, "true");
    sdcProperties.setProperty(
        RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG,
        Configuration.FileRef.DELIMITER + appTokenFile.getAbsolutePath() + Configuration.FileRef.DELIMITER
    );
    sparkProvider.copyDpmTokenIfRequired(sdcProperties, etcDir);
    Assert.assertEquals("true", sdcProperties.getProperty(RemoteSSOService.DPM_ENABLED));
    Assert.assertEquals(Configuration.FileRef.DELIMITER +
            ClusterProviderImpl.CLUSTER_DPM_APP_TOKEN + Configuration.FileRef.DELIMITER,
        sdcProperties.getProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG)
    );
  }

  @Test
  public void testExclude() throws Throwable {
    Assert.assertTrue(ClusterProviderImpl.exclude(Arrays.asList("scala.*"), "scala-library.jar"));
    Assert.assertFalse(ClusterProviderImpl.exclude(Arrays.asList("^scala.*"), "Xscala-library.jar"));
  }
}
