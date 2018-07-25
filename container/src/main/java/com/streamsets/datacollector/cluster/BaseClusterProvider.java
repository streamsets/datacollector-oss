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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.creation.StageLibraryDelegateCreator;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.lineage.LineagePublisherConstants;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryUtils;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.impl.FileAclStoreTask;
import com.streamsets.datacollector.store.impl.FilePipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.lib.security.acl.AclDtoJsonMapper;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.delegate.exported.ClusterJob;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.util.SystemProcess;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.streamsets.datacollector.definition.StageLibraryDefinitionExtractor.DATA_COLLECTOR_LIBRARY_PROPERTIES;
import static java.util.Arrays.stream;

public abstract class BaseClusterProvider implements ClusterProvider {
  static final Pattern YARN_APPLICATION_ID_REGEX = Pattern.compile("\\s(application_[0-9]+_[0-9]+)(\\s|$)");
  static final Pattern MESOS_DRIVER_ID_REGEX = Pattern.compile("\\s(driver-[0-9]+-[0-9]+)(\\s|$)");
  static final Pattern NO_VALID_CREDENTIALS = Pattern.compile("(No valid credentials provided.*)");
  static final String CLUSTER_DPM_APP_TOKEN = "cluster-application-token.txt";
  public static final String CLUSTER_TYPE = "CLUSTER_TYPE";
  public static final String CLUSTER_TYPE_MESOS = "mesos";
  public static final String CLUSTER_TYPE_MAPREDUCE = "mr";
  public static final String CLUSTER_TYPE_YARN = "yarn";
  private static final String CLUSTER_MODE_JAR_BLACKLIST = "cluster.jar.blacklist.regex_";

  // Comma separated list of sdc.properties configs that should not be passed from master sdc to slave sdcs
  private static final String CONFIG_ADDITIONAL_CONFIGS_TO_REMOVE = "cluster.slave.configs.remove";
  // List of properties that we want to always remove as they do not make sense when passed from master sdc to slave sdcs
  private static final String []SDC_CONFIGS_TO_ALWAYS_REMOVE = {
    RuntimeInfo.DATA_COLLECTOR_BASE_HTTP_URL,
    "http.bindHost"
  };

  static final String CLUSTER_BOOTSTRAP_JAR_REGEX = "cluster.bootstrap.jar.regex_";
  static final Pattern CLUSTER_BOOTSTRAP_API_JAR_PATTERN = Pattern.compile(
      "streamsets-datacollector-cluster-bootstrap-api-\\d+.*.jar$");
  static final Pattern BOOTSTRAP_MAIN_JAR_PATTERN = Pattern.compile("streamsets-datacollector-bootstrap-\\d+.*.jar$");
  static final Pattern CLUSTER_BOOTSTRAP_JAR_PATTERN = Pattern.compile
      ("streamsets-datacollector-cluster-bootstrap-\\d+.*.jar$");
  static final Pattern CLUSTER_BOOTSTRAP_MESOS_JAR_PATTERN = Pattern.compile
      ("streamsets-datacollector-mesos-bootstrap-\\d+.*.jar$");
  private static final String ALL_STAGES = "*";
  private static final String TOPIC = "topic";
  private static final String MESOS_HOSTING_DIR_PARENT = "mesos";
  public static final String SPARK_PROCESSOR_STAGE = "com.streamsets.pipeline.stage.processor.spark.SparkDProcessor";
  private static final String YARN_SPARK_APP_LOG_PATH = "${spark.yarn.app.container.log.dir}/sdc.log";
  private static final String YARN_MAPREDUCE_APP_LOG_PATH = "${yarn.app.container.log.dir}/sdc.log";

  @VisibleForTesting
  static final Map<ExecutionMode, String> executionModeToAppLogPath =
      new ImmutableMap.Builder<ExecutionMode, String>().
          put(ExecutionMode.CLUSTER_YARN_STREAMING, YARN_SPARK_APP_LOG_PATH).
          put(ExecutionMode.CLUSTER_BATCH, YARN_MAPREDUCE_APP_LOG_PATH).build();


  private final static Logger LOG = LoggerFactory.getLogger(BaseClusterProvider.class);

  private final RuntimeInfo runtimeInfo;
  /**
   * Only null in the case of tests
   */
  @Nullable
  private final SecurityConfiguration securityConfiguration;

  private final Configuration configuration;

  private final Logger log;

  private final StageLibraryTask stageLibraryTask;

  public BaseClusterProvider(RuntimeInfo runtimeInfo,
                             @Nullable SecurityConfiguration securityConfiguration,
                             Configuration conf,
                             StageLibraryTask stageLibraryTask) {
    log = LoggerFactory.getLogger(getClass());
    this.runtimeInfo = runtimeInfo;
    this.stageLibraryTask = stageLibraryTask;
    this.securityConfiguration = securityConfiguration;
    this.configuration = conf;
  }


  protected ClusterJob getClusterJobDelegator(PipelineConfiguration pipelineConfiguration) {
    PipelineBean pipelineBean = PipelineBeanCreator.get().create(false,
        stageLibraryTask,
        pipelineConfiguration,
        null,
        new ArrayList<>()
    );
    StageConfiguration stageConf = pipelineBean.getOrigin().getConfiguration();
    StageDefinition stageDef = stageLibraryTask.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
    return StageLibraryDelegateCreator.get().createAndInitialize(stageLibraryTask,
        configuration,
        stageDef.getLibrary(),
        ClusterJob.class
    );
  }

  protected RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  public SecurityConfiguration getSecurityConfiguration() {
    return securityConfiguration;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  protected boolean isIsTraceEnabled() {
    return getLog().isTraceEnabled();
  }

  protected Logger getLog() {
    return log;
  }

  protected static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }

  @VisibleForTesting
  void rewriteProperties(
      File sdcPropertiesFile,
      File etcStagingDir,
      Map<String, String> sourceConfigs,
      Map<String, String> sourceInfo,
      String clusterToken,
      Optional<String> mesosURL
  ) throws IOException {
    InputStream sdcInStream = null;
    OutputStream sdcOutStream = null;
    Properties sdcProperties = new Properties();
    try {
      sdcInStream = new FileInputStream(sdcPropertiesFile);
      sdcProperties.load(sdcInStream);
      copyDpmTokenIfRequired(sdcProperties, etcStagingDir);
      sdcProperties.setProperty(RuntimeModule.PIPELINE_EXECUTION_MODE_KEY, ExecutionMode.SLAVE.name());
      sdcProperties.setProperty(WebServerTask.REALM_FILE_PERMISSION_CHECK, "false");

      // Remove always problematical properties
      for(String property: SDC_CONFIGS_TO_ALWAYS_REMOVE) {
        sdcProperties.remove(property);
      }

      // Remove additional properties that user might need to
      String propertiesToRemove = sdcProperties.getProperty(CONFIG_ADDITIONAL_CONFIGS_TO_REMOVE);
      if(propertiesToRemove != null) {
        for(String property : propertiesToRemove.split(",")) {
          sdcProperties.remove(property);
        }
      }

      if (runtimeInfo != null) {
        if (runtimeInfo.getSSLContext() != null) {
          sdcProperties.setProperty(WebServerTask.HTTP_PORT_KEY, "-1");
          sdcProperties.setProperty(WebServerTask.HTTPS_PORT_KEY, "0");
        } else {
          sdcProperties.setProperty(WebServerTask.HTTP_PORT_KEY, "0");
          sdcProperties.setProperty(WebServerTask.HTTPS_PORT_KEY, "-1");
        }
        String id = String.valueOf(runtimeInfo.getId());
        sdcProperties.setProperty(Constants.SDC_ID, id);
        sdcProperties.setProperty(Constants.PIPELINE_CLUSTER_TOKEN_KEY, clusterToken);
        sdcProperties.setProperty(Constants.CALLBACK_SERVER_URL_KEY, runtimeInfo.getClusterCallbackURL());
      }

      if (mesosURL.isPresent()) {
        sdcProperties.setProperty(Constants.MESOS_JAR_URL, mesosURL.get());
      }
      addClusterConfigs(sourceConfigs, sdcProperties);
      addClusterConfigs(sourceInfo, sdcProperties);

      sdcOutStream = new FileOutputStream(sdcPropertiesFile);
      sdcProperties.store(sdcOutStream, null);
      getLog().debug("sourceConfigs = {}", sourceConfigs);
      getLog().debug("sourceInfo = {}", sourceInfo);
      getLog().debug("sdcProperties = {}", sdcProperties);
      sdcOutStream.flush();
      sdcOutStream.close();
    } finally {
      if (sdcInStream != null) {
        IOUtils.closeQuietly(sdcInStream);
      }
      if (sdcOutStream != null) {
        IOUtils.closeQuietly(sdcOutStream);
      }
    }
  }

  private void addClusterConfigs(Map<String, String> configs, Properties properties) {
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
  }

  private static File getBootstrapClusterJar(File bootstrapDir, final Pattern pattern) {
    File clusterBootstrapDir = new File(bootstrapDir, "cluster");
    return getBootstrapJar(clusterBootstrapDir, pattern);
  }

  private static File getBootstrapMainJar(File bootstrapDir, final Pattern pattern) {
    File bootstrapMainDir = new File(bootstrapDir, "main");
    return getBootstrapJar(bootstrapMainDir, pattern);
  }

  private static File getBootstrapJar(File bootstrapDir, final Pattern pattern) {
    Utils.checkState(
        bootstrapDir.isDirectory(),
        Utils.format("SDC bootstrap cluster lib does not exist: {}", bootstrapDir)
    );
    File[] candidates = bootstrapDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File candidate) {
        return pattern.matcher(candidate.getName()).matches();
      }
    });
    Utils.checkState(candidates != null, Utils.format("Did not find jar matching {} in {}", pattern, bootstrapDir));
    Utils.checkState(candidates.length == 1, Utils.format("Did not find exactly one bootstrap jar: {}",
        Arrays.toString(candidates)));
    return candidates[0];
  }


  static File createDirectoryClone(File srcDir, String dirName, File tempDir) throws IOException {
    File tempSrcDir = new File(tempDir, dirName);
    FileUtils.deleteQuietly(tempSrcDir);
    Utils.checkState(tempSrcDir.mkdir(), Utils.formatL("Could not create {}", tempSrcDir));
    doCopyDirectory(srcDir, tempSrcDir);
    return tempSrcDir;
  }

  private static void doCopyDirectory(File srcDir, File destDir)
      throws IOException {
    // code copied from commons-io FileUtils to work around files which cannot be read
    // recurse
    final File[] srcFiles = srcDir.listFiles();
    if (srcFiles == null) {  // null if abstract pathname does not denote a directory, or if an I/O error occurs
      throw new IOException("Failed to list contents of " + srcDir);
    }
    if (destDir.exists()) {
      if (!destDir.isDirectory()) {
        throw new IOException("Destination '" + destDir + "' exists but is not a directory");
      }
    } else {
      if (!destDir.mkdirs() && !destDir.isDirectory()) {
        throw new IOException("Destination '" + destDir + "' directory cannot be created");
      }
    }
    if (!destDir.canWrite()) {
      throw new IOException("Destination '" + destDir + "' cannot be written to");
    }
    for (final File srcFile : srcFiles) {
      final File dstFile = new File(destDir, srcFile.getName());
      if (srcFile.canRead()) { // ignore files which cannot be read
        if (srcFile.isDirectory()) {
          doCopyDirectory(srcFile, dstFile);
        } else {
          try (InputStream in = new FileInputStream((srcFile))) {
            try (OutputStream out = new FileOutputStream((dstFile))) {
              IOUtils.copy(in, out);
            }
          }
        }
      }
    }
  }


  static boolean exclude(List<String> blacklist, String name) {
    for (String pattern : blacklist) {
      if (Pattern.compile(pattern).matcher(name).find()) {
        return true;
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("Pattern '{}' does not match '{}'", pattern, name);
      }
    }
    return false;
  }

  @VisibleForTesting
  static Properties readDataCollectorProperties(ClassLoader cl) throws IOException {
    Properties properties = new Properties();
    while (cl != null) {
      Enumeration<URL> urls = cl.getResources(DATA_COLLECTOR_LIBRARY_PROPERTIES);
      if (urls != null) {
        while (urls.hasMoreElements()) {
          URL url = urls.nextElement();
          LOG.trace("Loading data collector library properties: {}", url);
          try (InputStream inputStream = url.openStream()) {
            properties.load(inputStream);
          }
        }
      }
      cl = cl.getParent();
    }
    LOG.trace("Final properties: {} ", properties);
    return properties;
  }

  private List<URL> findJars(String name, URLClassLoader cl, @Nullable String stageClazzName)
      throws IOException {
    Properties properties = readDataCollectorProperties(cl);
    List<String> blacklist = new ArrayList<>();
    for (Map.Entry entry : properties.entrySet()) {
      String key = (String) entry.getKey();
      if (stageClazzName != null && key.equals(CLUSTER_MODE_JAR_BLACKLIST + stageClazzName)) {
        String value = (String) entry.getValue();
        blacklist.addAll(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(value));
      } else if (key.equals(CLUSTER_MODE_JAR_BLACKLIST + ALL_STAGES)) {
        String value = (String) entry.getValue();
        blacklist.addAll(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(value));
      }
    }
    if (isIsTraceEnabled()) {
      getLog().trace("Blacklist for '{}': '{}'", name, blacklist);
    }
    List<URL> urls = new ArrayList<>();
    for (URL url : cl.getURLs()) {
      if (blacklist.isEmpty()) {
        urls.add(url);
      } else {
        if (exclude(blacklist, FilenameUtils.getName(url.getPath()))) {
          getLog().trace("Skipping '{}' for '{}' due to '{}'", url, name, blacklist);
        } else {
          urls.add(url);
        }
      }
    }
    return urls;
  }

  @Override
  public ApplicationState startPipeline(
      File outputDir,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean,
      StageLibraryTask stageLibrary,
      CredentialStoresTask credentialStoresTask,
      File etcDir,
      File resourcesDir,
      File staticWebDir, File bootstrapDir,
      URLClassLoader apiCL,
      URLClassLoader containerCL,
      long timeToWaitForFailure,
      RuleDefinitions ruleDefinitions,
      Acl acl
  ) throws IOException, TimeoutException, StageException {
    File stagingDir = new File(outputDir, "staging");
    if (!stagingDir.mkdirs() || !stagingDir.isDirectory()) {
      String msg = Utils.format("Could not create staging directory: {}", stagingDir);
      throw new IllegalStateException(msg);
    }
    try {
      return startPipelineInternal(
          outputDir,
          sourceInfo,
          pipelineConfiguration,
          pipelineConfigBean,
          stageLibrary,
          credentialStoresTask,
          etcDir,
          resourcesDir,
          staticWebDir,
          bootstrapDir,
          apiCL,
          containerCL,
          timeToWaitForFailure,
          stagingDir,
          ruleDefinitions,
          acl
      );
    } finally {
      // in testing mode the staging dir is used by yarn
      // tasks and thus cannot be deleted
      if (!Boolean.getBoolean("sdc.testing-mode") && !FileUtils.deleteQuietly(stagingDir)) {
        getLog().warn("Unable to cleanup: {}", stagingDir);
      }
    }
  }

  private List<String> getLog4jConfig(ExecutionMode executionMode, String clusterLog4jFile) throws IOException {
    List<String> log4jConfigs;
    // Keep logging to stderr as default for keeping compatibility with Spark UI. Remove in next major release.
    if (Boolean.valueOf(configuration.get("cluster.pipelines.logging.to.stderr", "true"))) {
      log4jConfigs = ClusterLogConfigUtils.getLogContent(runtimeInfo, clusterLog4jFile);
    } else {
      log4jConfigs = ClusterLogConfigUtils.getLog4jConfigAndAddAppender(runtimeInfo, executionModeToAppLogPath.get(executionMode));
    }
    return log4jConfigs;
  }

  @SuppressWarnings("unchecked")
  private ApplicationState startPipelineInternal(
      File outputDir,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean,
      StageLibraryTask stageLibrary,
      CredentialStoresTask credentialStoresTask,
      File etcDir,
      File resourcesDir,
      File staticWebDir,
      File bootstrapDir,
      URLClassLoader apiCL,
      URLClassLoader containerCL,
      long timeToWaitForFailure,
      File stagingDir,
      RuleDefinitions ruleDefinitions,
      Acl acl
  ) throws IOException, TimeoutException, StageException {

    // create libs.tar.gz file for pipeline
    Map<String, List<URL>> streamsetsLibsCl = new HashMap<>();
    Map<String, List<URL>> userLibsCL = new HashMap<>();
    Map<String, String> sourceConfigs = new HashMap<>();
    ImmutableList.Builder<StageConfiguration> pipelineConfigurations = ImmutableList.builder();
    // order is important here as we don't want error stage
    // configs overriding source stage configs
    String clusterToken = UUID.randomUUID().toString();
    Set<String> jarsToShip = new LinkedHashSet<>();
    List<Issue> errors = new ArrayList<>();
    PipelineBean pipelineBean = PipelineBeanCreator.get().create(
        false,
        stageLibrary,
        pipelineConfiguration,
        null,
        errors
    );
    if (!errors.isEmpty()) {
      String msg = Utils.format("Found '{}' configuration errors: {}", errors.size(), errors);
      throw new IllegalStateException(msg);
    }
    pipelineConfigurations.add(pipelineBean.getErrorStage().getConfiguration());
    StageBean statsStage = pipelineBean.getStatsAggregatorStage();
    // statsStage is null for pre 1.3 pipelines
    if (statsStage != null) {
      pipelineConfigurations.add(statsStage.getConfiguration());
    }
    pipelineConfigurations.add(pipelineBean.getOrigin().getConfiguration());
    for (StageBean stageBean : pipelineBean.getPipelineStageBeans().getStages()) {
      pipelineConfigurations.add(stageBean.getConfiguration());
    }

    ExecutionMode executionMode = ExecutionMode.STANDALONE;
    for (StageConfiguration stageConf : pipelineConfigurations.build()) {
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(), false);
      if (stageConf.getInputLanes().isEmpty()) {
        for (Config conf : stageConf.getConfiguration()) {
          if (conf.getValue() != null) {
            Object value = conf.getValue();
            if (value instanceof List) {
              List values = (List) value;
              if (values.isEmpty()) {
                getLog().debug("Conf value for " + conf.getName() + " is empty");
              } else {
                Object first = values.get(0);
                if (canCastToString(first)) {
                  sourceConfigs.put(conf.getName(), Joiner.on(",").join(values));
                } else if (first instanceof Map) {
                  addToSourceConfigs(sourceConfigs, (List<Map<String, Object>>) values);
                } else {
                  getLog().info(
                      "List is of type '{}' which cannot be converted to property value.",
                      first.getClass().getName()
                  );
                }
              }
            } else if (canCastToString(conf.getValue())) {
              sourceConfigs.put(conf.getName(), String.valueOf(value));
            } else if (value instanceof Enum) {
              value = ((Enum) value).name();
              sourceConfigs.put(conf.getName(), String.valueOf(value));
            } else {
              getLog().warn("Conf value is of unknown type " + conf.getValue());
            }
          }
        }
        executionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConfiguration, new ArrayList<Issue>());

        List<String> libJarsRegex = stageDef.getLibJarsRegex();
        if (!libJarsRegex.isEmpty()) {
          for (URL jarUrl : ((URLClassLoader) stageDef.getStageClassLoader()).getURLs()) {
            File jarFile = new File(jarUrl.getPath());
            for (String libJar : libJarsRegex) {
              Pattern pattern = Pattern.compile(libJar);
              Matcher matcher = pattern.matcher(jarFile.getName());
              if (matcher.matches()) {
                jarsToShip.add(jarFile.getAbsolutePath());
              }
            }
          }
        }
      }

      // Add stage own stage library to the jars that needs to be shipped
      extractClassLoaderInfo(streamsetsLibsCl, userLibsCL, stageDef.getStageClassLoader(), stageDef.getClassName());

      // TODO: Get extras dir from the env var.
      // Then traverse each jar's parent (getParent method) and add only the ones who has the extras dir as parent.
      // Add all jars of stagelib to --jars. We only really need stuff from the extras directory.
      if (stageDef.getClassName().equals(SPARK_PROCESSOR_STAGE)) {
        getLog().info("Spark processor found in pipeline, adding to spark-submit");
        File extras = new File(System.getenv("STREAMSETS_LIBRARIES_EXTRA_DIR"));
        getLog().info("Found extras dir: " + extras.toString());
        File stageLibExtras = new File(extras.toString() + "/" + stageConf.getLibrary() + "/" + "lib");
        getLog().info("StageLib Extras dir: " + stageLibExtras.toString());
        File[] extraJarsForStageLib = stageLibExtras.listFiles();
        if (extraJarsForStageLib != null) {
          stream(extraJarsForStageLib).map(File::toString).forEach(jarsToShip::add);
        }
        addJarsToJarsList((URLClassLoader) stageDef.getStageClassLoader(), jarsToShip, "streamsets-datacollector-spark-api-[0-9]+.*");
      }
    }
    for (CredentialStoreDefinition credentialStoreDefinition : credentialStoresTask.getConfiguredStoreDefinititions()) {
      getLog().info(
          "Adding Credential store stage library for: {}",
          credentialStoreDefinition.getName()
     );
      extractClassLoaderInfo(streamsetsLibsCl,
          userLibsCL,
          credentialStoreDefinition.getStageLibraryDefinition().getClassLoader(),
          credentialStoreDefinition.getStoreClass().getName()
      );
    }


    // We're shipping several stage libraries to the backend and those libraries can have stages that depends on various
    // different services. Our bootstrap procedure will however terminate SDC start up if we have stage that doesn't have
    // all required services available. Hence we go through all the stages that are being sent and ship all their
    // services to the cluster as well.
    for(StageDefinition stageDef: stageLibrary.getStages()) {
      String stageLibName = stageDef.getLibrary();
      if(streamsetsLibsCl.containsKey(stageLibName) || userLibsCL.containsKey(stageLibName)) {
        for(ServiceDependencyDefinition serviceDep : stageDef.getServices()) {
          ServiceDefinition serviceDef = stageLibrary.getServiceDefinition(serviceDep.getService(), false);
          getLog().debug("Adding service {} for stage {}", serviceDef.getClassName(), stageDef.getName());
          extractClassLoaderInfo(streamsetsLibsCl, userLibsCL, serviceDef.getStageClassLoader(), serviceDef.getClassName());
        }
      }
    }

    if(configuration != null && configuration.hasName(LineagePublisherConstants.CONFIG_LINEAGE_PUBLISHERS)) {
      String confDefName = LineagePublisherConstants.configDef(configuration.get(LineagePublisherConstants.CONFIG_LINEAGE_PUBLISHERS, null));
      String lineagePublisherDef = configuration.get(confDefName, null);
      if (lineagePublisherDef != null) {
        String[] configDef = lineagePublisherDef.split("::");
        LineagePublisherDefinition def = stageLibrary.getLineagePublisherDefinition(configDef[0], configDef[1]);
        getLog().debug("Adding Lineage Publisher {}:{}", def.getClassLoader(), def.getKlass().getName());
        extractClassLoaderInfo(streamsetsLibsCl, userLibsCL, def.getClassLoader(), def.getKlass().getName());
      }
    }

    if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING ||
        executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      getLog().info("Execution Mode is CLUSTER_STREAMING. Adding container jar and API jar to spark-submit");
      addJarsToJarsList(containerCL, jarsToShip, "streamsets-datacollector-container-[0-9]+.*");
      // EscapeUtil is required by RecordImpl#get() and RecordImpl#set(), and has no additional dependencies, so
      // ship this as well.
      addJarsToJarsList(containerCL, jarsToShip, "streamsets-datacollector-common-[0-9]+.*");
      addJarsToJarsList(apiCL, jarsToShip, "streamsets-datacollector-api-[0-9]+.*");
    }

    getLog().info("stagingDir = '{}'", stagingDir);
    getLog().info("bootstrapDir = '{}'", bootstrapDir);
    getLog().info("etcDir = '{}'", etcDir);
    getLog().info("resourcesDir = '{}'", resourcesDir);
    getLog().info("staticWebDir = '{}'", staticWebDir);

    Utils.checkState(staticWebDir.isDirectory(), Utils.format("Expected '{}' to be a directory", staticWebDir));
    File libsTarGz = new File(stagingDir, "libs.tar.gz");
    try {
      TarFileCreator.createLibsTarGz(
          findJars("api", apiCL, null),
          findJars("container", containerCL, null),
          streamsetsLibsCl,
          userLibsCL,
          staticWebDir,
          libsTarGz
      );
    } catch (Exception ex) {
      String msg = errorString("Serializing classpath: '{}'", ex);
      throw new RuntimeException(msg, ex);
    }
    File resourcesTarGz = new File(stagingDir, "resources.tar.gz");
    try {
      resourcesDir = createDirectoryClone(resourcesDir, "resources", stagingDir);
      TarFileCreator.createTarGz(resourcesDir, resourcesTarGz);
    } catch (Exception ex) {
      String msg = errorString("Serializing resources directory: '{}': {}", resourcesDir.getName(), ex);
      throw new RuntimeException(msg, ex);
    }
    File etcTarGz = new File(stagingDir, "etc.tar.gz");
    File sdcPropertiesFile;
    File bootstrapJar = getBootstrapMainJar(bootstrapDir, BOOTSTRAP_MAIN_JAR_PATTERN);
    File clusterBootstrapJar;
    String mesosHostingJarDir = null;
    String mesosURL = null;
    Pattern clusterBootstrapJarFile = findClusterBootstrapJar(executionMode, pipelineConfiguration, stageLibrary);
    clusterBootstrapJar = getBootstrapClusterJar(bootstrapDir, clusterBootstrapJarFile);
    String clusterBootstrapApiJar = getBootstrapClusterJar(bootstrapDir, CLUSTER_BOOTSTRAP_API_JAR_PATTERN).getAbsolutePath();
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      String topic = sourceConfigs.get(TOPIC);
      String pipelineName = sourceInfo.get(ClusterModeConstants.CLUSTER_PIPELINE_NAME);
      mesosHostingJarDir = MESOS_HOSTING_DIR_PARENT + File.separatorChar + getSha256(getMesosHostingDir(topic, pipelineName));
      mesosURL = runtimeInfo.getBaseHttpUrl() + File.separatorChar + mesosHostingJarDir + File.separatorChar
                 + clusterBootstrapJar.getName();
    } else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
      jarsToShip.add(clusterBootstrapJar.getAbsolutePath());
    }

    try {
      etcDir = createDirectoryClone(etcDir, "etc", stagingDir);
      if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
        List<String> logLines = ClusterLogConfigUtils.getLogContent(runtimeInfo, "/cluster-spark-log4j.properties");
        File log4jProperty = new File(etcDir, runtimeInfo.getLog4jPropertiesFileName());
        if (!log4jProperty.isFile()) {
          throw new IllegalStateException(Utils.format(
              "Log4j config file doesn't exist: '{}'",
              log4jProperty.getAbsolutePath()
          ));
        }
        Files.write(log4jProperty.toPath(), logLines, Charset.defaultCharset());
      }
      PipelineInfo pipelineInfo = Utils.checkNotNull(pipelineConfiguration.getInfo(), "Pipeline Info");
      String pipelineName = pipelineInfo.getPipelineId();
      File rootDataDir = new File(etcDir, "data");
      File pipelineBaseDir = new File(rootDataDir, PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
      File pipelineDir = new File(pipelineBaseDir, PipelineUtils.escapedPipelineName(pipelineName));
      if (!pipelineDir.exists()) {
        if (!pipelineDir.mkdirs()) {
          throw new RuntimeException("Failed to create pipeline directory " + pipelineDir.getPath());
        }
      }
      File pipelineRunInfoDir = new File(new File(new File(rootDataDir, PipelineDirectoryUtil.PIPELINE_BASE_DIR),
          PipelineUtils.escapedPipelineName(pipelineName)
      ), "0");
      if (!pipelineRunInfoDir.mkdirs()) {
        throw new RuntimeException(Utils.format(
            "Failed to create pipeline directory: '{}'",
            pipelineRunInfoDir.getPath()
        ));
      }
      File pipelineFile = new File(pipelineDir, FilePipelineStoreTask.PIPELINE_FILE);
      ObjectMapperFactory.getOneLine().writeValue(pipelineFile,
          BeanHelper.wrapPipelineConfiguration(pipelineConfiguration));
      File infoFile = new File(pipelineDir, FilePipelineStoreTask.INFO_FILE);
      ObjectMapperFactory.getOneLine().writeValue(infoFile, BeanHelper.wrapPipelineInfo(pipelineInfo));
      Utils.checkNotNull(ruleDefinitions, "ruleDefinitions");
      File rulesFile = new File(pipelineDir, FilePipelineStoreTask.RULES_FILE);
      ObjectMapperFactory.getOneLine().writeValue(rulesFile, BeanHelper.wrapRuleDefinitions(ruleDefinitions));
      if (null != acl) {
        // acl could be null if permissions is not enabled
        File aclFile = new File(pipelineDir, FileAclStoreTask.ACL_FILE);
        ObjectMapperFactory.getOneLine().writeValue(aclFile, AclDtoJsonMapper.INSTANCE.toAclJson(acl));
      }
      sdcPropertiesFile = new File(etcDir, "sdc.properties");
      if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
        String hdfsS3ConfDirValue = PipelineBeanCreator.get().getHdfsS3ConfDirectory(pipelineConfiguration);
        if (hdfsS3ConfDirValue != null && !hdfsS3ConfDirValue.isEmpty()) {
          File hdfsS3ConfDir = new File(resourcesDir, hdfsS3ConfDirValue).getAbsoluteFile();
          if (!hdfsS3ConfDir.exists()) {
            String msg = Utils.format("HDFS/S3 Checkpoint Configuration Directory '{}' doesn't exist",
              hdfsS3ConfDir.getPath());
            throw new IllegalArgumentException(msg);
          } else {
            File coreSite = new File(hdfsS3ConfDir, "core-site.xml");
            if (!coreSite.exists()) {
              String msg = Utils.format("HDFS/S3 Checkpoint Configuration file core-site.xml '{}' doesn't exist",
                coreSite.getPath());
              throw new IllegalStateException(msg);
            }
            sourceConfigs.put("hdfsS3ConfDir", hdfsS3ConfDirValue);
          }
        } else {
          throw new IllegalStateException("HDFS/S3 Checkpoint configuration directory is required");
        }
      }
      rewriteProperties(sdcPropertiesFile, etcDir, sourceConfigs, sourceInfo, clusterToken, Optional.ofNullable(mesosURL));
      TarFileCreator.createTarGz(etcDir, etcTarGz);
    } catch (IOException | RuntimeException ex) {
      String msg = errorString("Error while preparing for cluster job submission: {}", ex);
      throw new RuntimeException(msg, ex);
    }
    File log4jProperties = new File(stagingDir, "log4j.properties");
    InputStream clusterLog4jProperties = null;
    try {
      List<String> lines = null;
      if (executionMode == ExecutionMode.CLUSTER_BATCH) {
        lines = getLog4jConfig(executionMode, "/cluster-mr-log4j.properties");
      } else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
        lines = getLog4jConfig(executionMode, "/cluster-spark-log4j.properties");
      }
      if (lines != null) {
        Files.write(log4jProperties.toPath(), lines, Charset.defaultCharset());
      }
    } catch (IOException ex) {
      String msg = errorString("copying log4j configuration: {}", ex);
      throw new RuntimeException(msg, ex);
    } finally {
      if (clusterLog4jProperties != null) {
        IOUtils.closeQuietly(clusterLog4jProperties);
      }
    }
    return startPipelineExecute(
        outputDir,
        sourceInfo,
        pipelineConfiguration,
        pipelineConfigBean,
        timeToWaitForFailure,
        stagingDir, //* required by shell script
        clusterToken,

        clusterBootstrapJar, //* Main Jar
        bootstrapJar, //* local JAR 1
        jarsToShip, //* local JARs 2+

        libsTarGz, //* archive
        resourcesTarGz, //* archive
        etcTarGz, //* archive

        sdcPropertiesFile, //* needed for driver invocation

        log4jProperties, //* need for driver invocation

        mesosHostingJarDir,
        mesosURL,

        clusterBootstrapApiJar,

        errors
    );
  }

  protected abstract ApplicationState startPipelineExecute(
      File outputDir,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean,
      long timeToWaitForFailure,

      File stagingDir,

      String clusterToken,

      File clusterBootstrapJar,
      File bootstrapJar,
      Set<String> jarsToShip,

      File libsTarGz,
      File resourcesTarGz,
      File etcTarGz,
      File sdcPropertiesFile,
      File log4jProperties,

      String mesosHostingJarDir,
      String mesosURL,
      String clusterBootstrapApiJar,

      List<Issue> errors
  ) throws IOException, StageException;


  private void extractClassLoaderInfo(
      Map<String, List<URL>> streamsetsLibsCl,
      Map<String, List<URL>> userLibsCL,
      ClassLoader cl,
      String mainClass
  ) throws IOException {
    String type = StageLibraryUtils.getLibraryType(cl);
    String name = StageLibraryUtils.getLibraryName(cl);
    if (ClusterModeConstants.STREAMSETS_LIBS.equals(type)) {
      streamsetsLibsCl.put(name, findJars(name, (URLClassLoader) cl, mainClass));
      // sdc-user-libs for Navigator and Atlas, and the customer's custom stages, too.
    } else if (ClusterModeConstants.USER_LIBS.equals(type) || ClusterModeConstants.SDC_USER_LIBS.equals(type)) {
      userLibsCL.put(name, findJars(name, (URLClassLoader) cl, mainClass));
    } else {
      throw new IllegalStateException(Utils.format("Error unknown stage library type: '{}'", type));
    }
  }

  private void addJarsToJarsList(URLClassLoader cl, Set<String> jarsToShip, String regex) {
    jarsToShip.addAll(getFilesInCL(cl, regex));
  }

  private List<String> getFilesInCL(URLClassLoader cl, String regex) {
    List<String> files = new ArrayList<>();
    for (URL url : cl.getURLs()){
      File jar = new File(url.getPath());
      if (jar.getName().matches(regex)) {
        getLog().info(Utils.format("Adding {} to ship.", url.getPath()));
        files.add(jar.getAbsolutePath());
      }
    }
    return files;
  }

  @VisibleForTesting
  void copyDpmTokenIfRequired(Properties sdcProps, File etcStagingDir) throws IOException {
    String configFiles = sdcProps.getProperty(Configuration.CONFIG_INCLUDES);
    if (configFiles != null) {
      for (String include : Splitter.on(",").trimResults().omitEmptyStrings().split(configFiles)) {
        File file = new File(etcStagingDir, include);
        try (Reader reader = new FileReader(file)) {
          Properties includesDpmProps = new Properties();
          includesDpmProps.load(reader);
          if (copyDpmTokenIfEnabled(includesDpmProps, etcStagingDir, include)) {
            break;
          }
        }
      }
    } else { //config.includes won't be there for parcels installation, all configs in sdc.properties
      copyDpmTokenIfEnabled(sdcProps, etcStagingDir, null);
    }
  }

  private boolean copyDpmTokenIfEnabled(Properties props, File etcStagingDir, String include) throws IOException {
    Object isDPMEnabled = props.get(RemoteSSOService.DPM_ENABLED);
    if (isDPMEnabled != null) {
      if (Boolean.parseBoolean(((String) isDPMEnabled).trim())) {
        copyDpmTokenIfAbsolute(props, etcStagingDir);
        if (include != null) {
          try (OutputStream outputStream = new FileOutputStream(new File(etcStagingDir, include))) {
            props.store(outputStream, null);
          }
        }
        return true;
      }
    }
    return false;
  }

  private void copyDpmTokenIfAbsolute(Properties includesDpmProps, File etcStagingDir) throws IOException {
    String dpmTokenFile = includesDpmProps.getProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG);
    String tokenFile = Configuration.FileRef.getUnresolvedValueWithoutDelimiter(dpmTokenFile,
        Configuration.FileRef.PREFIX,
        Configuration.FileRef.SUFFIX,
        Configuration.FileRef.DELIMITER
    );
    if (Paths.get(tokenFile).isAbsolute()) {
      getLog().info("Copying application token from absolute location {} to etc's staging dir: {}",
          tokenFile,
          etcStagingDir
      );
      try (InputStream inStream = new FileInputStream((tokenFile))) {
        try (OutputStream out = new FileOutputStream(new File(etcStagingDir, CLUSTER_DPM_APP_TOKEN))) {
          IOUtils.copy(inStream, out);
        }
      }
      // set the property
      includesDpmProps.setProperty(RemoteSSOService.SECURITY_SERVICE_APP_AUTH_TOKEN_CONFIG,
          Configuration.FileRef.DELIMITER +
              CLUSTER_DPM_APP_TOKEN + Configuration.FileRef.DELIMITER
      );
    }
  }

  @VisibleForTesting
  Pattern findClusterBootstrapJar(
      ExecutionMode executionMode, PipelineConfiguration pipelineConf, StageLibraryTask stageLibraryTask
  ) throws IOException {
    StageConfiguration stageConf = PipelineConfigurationUtil.getSourceStageConf(pipelineConf);
    StageDefinition stageDefinition = stageLibraryTask.getStage(stageConf.getLibrary(),
        stageConf.getStageName(),
        false
    );
    ClassLoader stageClassLoader = stageDefinition.getStageClassLoader();
    Properties dataCollectorProps = readDataCollectorProperties(stageClassLoader);

    for (Map.Entry entry : dataCollectorProps.entrySet()) {
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      getLog().debug("Datacollector library properties key : '{}', value: '{}'", key, value);
      if (key.equals(CLUSTER_BOOTSTRAP_JAR_REGEX + executionMode + "_" + stageDefinition.getClassName())) {
        getLog().info("Using bootstrap jar pattern: '{}'", value);
        return Pattern.compile(value + "-\\d+.*");
      }
    }
    Pattern defaultJarPattern;
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      defaultJarPattern = CLUSTER_BOOTSTRAP_MESOS_JAR_PATTERN;
    } else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
      defaultJarPattern = CLUSTER_BOOTSTRAP_JAR_PATTERN;
    } else {
      defaultJarPattern = CLUSTER_BOOTSTRAP_API_JAR_PATTERN;
    }
    return defaultJarPattern;
  }

  private void addToSourceConfigs(Map<String, String> sourceConfigs, List<Map<String, Object>> arrayListValues) {
    for (Map<String, Object> map : arrayListValues) {
      String confKey = null;
      String confValue = null;
      for (Map.Entry<String, Object> mapEntry : map.entrySet()) {
        String mapKey = mapEntry.getKey();
        Object mapValue = mapEntry.getValue();
        switch (mapKey) {
          case "key":
            // Assuming the key is always string
            confKey = String.valueOf(mapValue);
            break;
          case "value":
            confValue = canCastToString(mapValue) ? String.valueOf(mapValue) : null;
            break;
          default:
            confKey = mapKey;
            confValue = canCastToString(mapValue) ? String.valueOf(mapValue) : null;
            break;
        }
        if (confKey != null && confValue != null) {
          sourceConfigs.put(confKey, confValue);
        }
      }
    }
  }

  private boolean canCastToString(Object value) {
    return value instanceof String || value instanceof Number || value.getClass().isPrimitive() ||
        value instanceof Boolean;
  }


  private enum ClusterOrigin {
    HDFS, KAFKA;
  }

  private String getMesosHostingDir(String topic, String pipelineName) {
    String sdcId = String.valueOf(runtimeInfo.getId());
    String mesosHostingDir = sdcId + File.separatorChar + topic + File.separatorChar + pipelineName;
    return mesosHostingDir;
  }

  private String getSha256(String mesosHostingDir) throws UnsupportedEncodingException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    md.update(mesosHostingDir.getBytes("UTF-8"));
    return Base64.encodeBase64URLSafeString(md.digest());
  }
}
