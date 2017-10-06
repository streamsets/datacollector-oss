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
import com.google.common.collect.Maps;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.StageBean;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.json.ObjectMapperFactory;
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
import com.streamsets.datacollector.util.SystemProcessFactory;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.lib.security.acl.AclDtoJsonMapper;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
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
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.streamsets.datacollector.definition.StageLibraryDefinitionExtractor.DATA_COLLECTOR_LIBRARY_PROPERTIES;
import static java.util.Arrays.stream;

public class ClusterProviderImpl implements ClusterProvider {
  static final Pattern YARN_APPLICATION_ID_REGEX = Pattern.compile("\\s(application_[0-9]+_[0-9]+)(\\s|$)");
  static final Pattern MESOS_DRIVER_ID_REGEX = Pattern.compile("\\s(driver-[0-9]+-[0-9]+)(\\s|$)");
  static final Pattern NO_VALID_CREDENTIALS = Pattern.compile("(No valid credentials provided.*)");
  static final String CLUSTER_DPM_APP_TOKEN = "cluster-application-token.txt";
  public static final String CLUSTER_TYPE = "CLUSTER_TYPE";
  public static final String CLUSTER_TYPE_MESOS = "mesos";
  public static final String CLUSTER_TYPE_MAPREDUCE = "mr";
  public static final String CLUSTER_TYPE_YARN = "yarn";
  private static final String STAGING_DIR = "STAGING_DIR";
  private static final String MESOS_UBER_JAR_PATH = "MESOS_UBER_JAR_PATH";
  private static final String MESOS_UBER_JAR = "MESOS_UBER_JAR";
  private static final String ETC_TAR_ARCHIVE = "ETC_TAR_ARCHIVE";
  private static final String LIBS_TAR_ARCHIVE = "LIBS_TAR_ARCHIVE";
  private static final String RESOURCES_TAR_ARCHIVE = "RESOURCES_TAR_ARCHIVE";
  private static final String MESOS_HOSTING_JAR_DIR = "MESOS_HOSTING_JAR_DIR";
  private static final String KERBEROS_AUTH = "KERBEROS_AUTH";
  private static final String KERBEROS_KEYTAB = "KERBEROS_KEYTAB";
  private static final String KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";
  private static final String CLUSTER_MODE_JAR_BLACKLIST = "cluster.jar.blacklist.regex_";
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
  private final RuntimeInfo runtimeInfo;
  private final YARNStatusParser yarnStatusParser;
  private final MesosStatusParser mesosStatusParser;
  /**
   * Only null in the case of tests
   */
  @Nullable
  private final SecurityConfiguration securityConfiguration;

  private static final Logger LOG = LoggerFactory.getLogger(ClusterProviderImpl.class);
  private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();

  @VisibleForTesting
  ClusterProviderImpl() {
    this(null, null);
  }

  public ClusterProviderImpl(RuntimeInfo runtimeInfo, @Nullable SecurityConfiguration securityConfiguration) {
    this.runtimeInfo = runtimeInfo;
    this.securityConfiguration = securityConfiguration;
    this.yarnStatusParser = new YARNStatusParser();
    this.mesosStatusParser = new MesosStatusParser();
  }

  @Override
  public void killPipeline(
      SystemProcessFactory systemProcessFactory,
      File sparkManager,
      File tempDir,
      String appId,
      PipelineConfiguration pipelineConfiguration
  ) throws TimeoutException, IOException {
    Map<String, String> environment = new HashMap<>();
    environment.put(CLUSTER_TYPE, CLUSTER_TYPE_YARN);
    addKerberosConfiguration(environment);
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add(sparkManager.getAbsolutePath());
    args.add("kill");
    args.add(appId);
    ExecutionMode executionMode = PipelineBeanCreator.get()
        .getExecutionMode(pipelineConfiguration, new ArrayList<Issue>());
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      addMesosArgs(pipelineConfiguration, environment, args);
    }
    SystemProcess process = systemProcessFactory
        .create(ClusterProviderImpl.class.getSimpleName(), tempDir, args.build());
    try {
      process.start(environment);
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        logOutput(appId, process);
        throw new TimeoutException(errorString("Kill command for {} timed out.", appId));
      }
    } finally {
      process.cleanup();
    }
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }

  private static void logOutput(String appId, SystemProcess process) {
    try {
      LOG.info("Status command standard error: {} ", Joiner.on("\n").join(process.getAllError()));
      LOG.info("Status command standard output: {} ", Joiner.on("\n").join(process.getAllOutput()));
    } catch (Exception e) {
      String msg = errorString("Could not read output of command '{}' for app {}: {}", process.getCommand(), appId, e);
      LOG.error(msg, e);
    }
  }

  @Override
  public ClusterPipelineStatus getStatus(
      SystemProcessFactory systemProcessFactory,
      File sparkManager,
      File tempDir,
      String appId,
      PipelineConfiguration pipelineConfiguration
  ) throws TimeoutException, IOException {

    Map<String, String> environment = new HashMap<>();
    environment.put(CLUSTER_TYPE, CLUSTER_TYPE_YARN);
    addKerberosConfiguration(environment);
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add(sparkManager.getAbsolutePath());
    args.add("status");
    args.add(appId);
    ExecutionMode executionMode = PipelineBeanCreator.get()
        .getExecutionMode(pipelineConfiguration, new ArrayList<Issue>());
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      addMesosArgs(pipelineConfiguration, environment, args);
    }
    SystemProcess process = systemProcessFactory
        .create(ClusterProviderImpl.class.getSimpleName(), tempDir, args.build());
    try {
      process.start(environment);
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        logOutput(appId, process);
        throw new TimeoutException(errorString("YARN status command for {} timed out.", appId));
      }
      if (process.exitValue() != 0) {
        logOutput(appId, process);
        throw new IllegalStateException(errorString("Status command for {} failed with exit code {}.", appId,
            process.exitValue()));
      }
      logOutput(appId, process);
      String status;
      if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
        status = mesosStatusParser.parseStatus(process.getAllOutput());
      } else {
        status = yarnStatusParser.parseStatus(process.getAllOutput());
      }
      return ClusterPipelineStatus.valueOf(status);
    } finally {
      process.cleanup();
    }
  }

  private void addMesosArgs(
      PipelineConfiguration pipelineConfiguration,
      Map<String, String> environment,
      ImmutableList.Builder<String> args
  ) {
    String mesosDispatcherURL = Utils.checkNotNull(
        PipelineBeanCreator.get().getMesosDispatcherURL(pipelineConfiguration), "mesosDispatcherURL"
    );
    environment.put(CLUSTER_TYPE, CLUSTER_TYPE_MESOS);
    args.add("--master");
    args.add(mesosDispatcherURL);
  }

  private void rewriteProperties(
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
      sdcProperties.remove(RuntimeInfo.DATA_COLLECTOR_BASE_HTTP_URL);
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
      LOG.debug("sourceConfigs = {}", sourceConfigs);
      LOG.debug("sourceInfo = {}", sourceInfo);
      LOG.debug("sdcProperties = {}", sdcProperties);
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

  private void addKerberosConfiguration(Map<String, String> environment) {
    if (securityConfiguration != null) {
      environment.put(KERBEROS_AUTH, String.valueOf(securityConfiguration.isKerberosEnabled()));
      if (securityConfiguration.isKerberosEnabled()) {
        environment.put(KERBEROS_PRINCIPAL, securityConfiguration.getKerberosPrincipal());
        environment.put(KERBEROS_KEYTAB, securityConfiguration.getKerberosKeytab());
      }
    }
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
      } else if (IS_TRACE_ENABLED) {
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

  private static List<URL> findJars(String name, URLClassLoader cl, @Nullable String stageClazzName)
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
    if (IS_TRACE_ENABLED) {
      LOG.trace("Blacklist for '{}': '{}'", name, blacklist);
    }
    List<URL> urls = new ArrayList<>();
    for (URL url : cl.getURLs()) {
      if (blacklist.isEmpty()) {
        urls.add(url);
      } else {
        if (exclude(blacklist, FilenameUtils.getName(url.getPath()))) {
          LOG.trace("Skipping '{}' for '{}' due to '{}'", url, name, blacklist);
        } else {
          urls.add(url);
        }
      }
    }
    return urls;
  }

  @Override
  public ApplicationState startPipeline(
      SystemProcessFactory systemProcessFactory,
      File clusterManager,
      File outputDir,
      Map<String, String> environment,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      StageLibraryTask stageLibrary,
      File etcDir, File resourcesDir,
      File staticWebDir, File bootstrapDir,
      URLClassLoader apiCL,
      URLClassLoader containerCL,
      long timeToWaitForFailure,
      RuleDefinitions ruleDefinitions,
      Acl acl
  ) throws IOException, TimeoutException {
    File stagingDir = new File(outputDir, "staging");
    if (!stagingDir.mkdirs() || !stagingDir.isDirectory()) {
      String msg = Utils.format("Could not create staging directory: {}", stagingDir);
      throw new IllegalStateException(msg);
    }
    try {
      return startPipelineInternal(
          systemProcessFactory,
          clusterManager,
          outputDir,
          environment,
          sourceInfo,
          pipelineConfiguration,
          stageLibrary,
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
        LOG.warn("Unable to cleanup: {}", stagingDir);
      }
    }
  }


  @SuppressWarnings("unchecked")
  private ApplicationState startPipelineInternal(
      SystemProcessFactory systemProcessFactory,
      File clusterManager,
      File outputDir,
      Map<String, String> environment,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      StageLibraryTask stageLibrary,
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
  ) throws IOException, TimeoutException {
    environment = Maps.newHashMap(environment);
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
    PipelineBean pipelineBean = PipelineBeanCreator.get().create(false, stageLibrary, pipelineConfiguration, errors);
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
                LOG.debug("Conf value for " + conf.getName() + " is empty");
              } else {
                Object first = values.get(0);
                if (canCastToString(first)) {
                  sourceConfigs.put(conf.getName(), Joiner.on(",").join(values));
                } else if (first instanceof Map) {
                  addToSourceConfigs(sourceConfigs, (List<Map<String, Object>>) values);
                } else {
                  LOG.info(
                      "List is of type '{}' which cannot be converted to property value.",
                      first.getClass().getName()
                  );
                }
              }
            } else if (canCastToString(conf.getValue())) {
              LOG.debug("Adding to source configs " + conf.getName() + "=" + value);
              sourceConfigs.put(conf.getName(), String.valueOf(value));
            } else if (value instanceof Enum) {
              value = ((Enum) value).name();
              LOG.debug("Adding to source configs " + conf.getName() + "=" + value);
              sourceConfigs.put(conf.getName(), String.valueOf(value));
            } else {
              LOG.warn("Conf value is of unknown type " + conf.getValue());
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

      String type = StageLibraryUtils.getLibraryType(stageDef.getStageClassLoader());
      String name = StageLibraryUtils.getLibraryName(stageDef.getStageClassLoader());
      if (ClusterModeConstants.STREAMSETS_LIBS.equals(type)) {
        streamsetsLibsCl.put(
            name, findJars(name, (URLClassLoader) stageDef.getStageClassLoader(), stageDef.getClassName())
        );
      } else if (ClusterModeConstants.USER_LIBS.equals(type)) {
        userLibsCL.put(name, findJars(name, (URLClassLoader) stageDef.getStageClassLoader(), stageDef.getClassName()));
      } else {
        throw new IllegalStateException(Utils.format("Error unknown stage library type: '{}'", type));
      }

      // TODO: Get extras dir from the env var.
      // Then traverse each jar's parent (getParent method) and add only the ones who has the extras dir as parent.
      // Add all jars of stagelib to --jars. We only really need stuff from the extras directory.
      if (stageDef.getClassName().equals(SPARK_PROCESSOR_STAGE)) {
        LOG.info("Spark processor found in pipeline, adding to spark-submit");
        File extras = new File(System.getenv("STREAMSETS_LIBRARIES_EXTRA_DIR"));
        LOG.info("Found extras dir: " + extras.toString());
        File stageLibExtras = new File(extras.toString() + "/" + stageConf.getLibrary() + "/" + "lib");
        LOG.info("StageLib Extras dir: " + stageLibExtras.toString());
        File[] extraJarsForStageLib = stageLibExtras.listFiles();
        if (extraJarsForStageLib != null) {
          stream(extraJarsForStageLib).map(File::toString).forEach(jarsToShip::add);
        }
        addJarsToJarsList((URLClassLoader) stageDef.getStageClassLoader(), jarsToShip, "streamsets-datacollector-spark-api-[0-9]+.*");
      }
    }

    if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING ||
        executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      LOG.info("Execution Mode is CLUSTER_STREAMING. Adding container jar and API jar to spark-submit");
      addJarsToJarsList(containerCL, jarsToShip, "streamsets-datacollector-container-[0-9]+.*");
      // EscapeUtil is required by RecordImpl#get() and RecordImpl#set(), and has no additional dependencies, so
      // ship this as well.
      addJarsToJarsList(containerCL, jarsToShip, "streamsets-datacollector-common-[0-9]+.*");
      addJarsToJarsList(apiCL, jarsToShip, "streamsets-datacollector-api-[0-9]+.*");
    }

    LOG.info("stagingDir = '{}'", stagingDir);
    LOG.info("bootstrapDir = '{}'", bootstrapDir);
    LOG.info("etcDir = '{}'", etcDir);
    LOG.info("resourcesDir = '{}'", resourcesDir);
    LOG.info("staticWebDir = '{}'", staticWebDir);

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
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      String topic = sourceConfigs.get(TOPIC);
      String pipelineName = sourceInfo.get(ClusterModeConstants.CLUSTER_PIPELINE_NAME);
      mesosHostingJarDir = MESOS_HOSTING_DIR_PARENT + File.separatorChar + getSha256(getMesosHostingDir(topic, pipelineName));
      mesosURL = runtimeInfo.getBaseHttpUrl() + File.separatorChar + mesosHostingJarDir + File.separatorChar
                 + clusterBootstrapJar.getName();
    } else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
      jarsToShip.add(getBootstrapClusterJar(bootstrapDir, CLUSTER_BOOTSTRAP_API_JAR_PATTERN).getAbsolutePath());
    }

    try {
      etcDir = createDirectoryClone(etcDir, "etc", stagingDir);
      if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
        try (
        InputStream clusterLog4jProperties = Utils.checkNotNull(getClass().getResourceAsStream("/cluster-spark-log4j.properties"), "Cluster Log4J Properties")
        ) {
          File log4jProperty = new File(etcDir, runtimeInfo.getLog4jPropertiesFileName());
          if (!log4jProperty.isFile()) {
            throw new IllegalStateException(
              Utils.format("Log4j config file doesn't exist: '{}'", log4jProperty.getAbsolutePath())
            );
          }
          LOG.info("Copying log4j properties for mesos cluster mode");
          FileUtils.copyInputStreamToFile(clusterLog4jProperties,
            log4jProperty);
        }
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
      rewriteProperties(sdcPropertiesFile, etcDir, sourceConfigs, sourceInfo, clusterToken, Optional.ofNullable
          (mesosURL));
      TarFileCreator.createTarGz(etcDir, etcTarGz);
    } catch (RuntimeException ex) {
      String msg = errorString("serializing etc directory: {}", ex);
      throw new RuntimeException(msg, ex);
    }
    File log4jProperties = new File(stagingDir, "log4j.properties");
    InputStream clusterLog4jProperties = null;
    try {
      if (executionMode == ExecutionMode.CLUSTER_BATCH) {
        clusterLog4jProperties = Utils.checkNotNull(
            getClass().getResourceAsStream("/cluster-mr-log4j.properties"), "Cluster Log4J Properties"
        );
      } else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
        clusterLog4jProperties =
            Utils.checkNotNull(
                getClass().getResourceAsStream("/cluster-spark-log4j.properties"), "Cluster Log4J Properties"
            );
      }
      if (clusterLog4jProperties != null) {
        FileUtils.copyInputStreamToFile(clusterLog4jProperties, log4jProperties);
      }
    } catch (IOException ex) {
      String msg = errorString("copying log4j configuration: {}", ex);
      throw new RuntimeException(msg, ex);
    } finally {
      if (clusterLog4jProperties != null) {
        IOUtils.closeQuietly(clusterLog4jProperties);
      }
    }
    addKerberosConfiguration(environment);
    errors.clear();
    PipelineConfigBean config = PipelineBeanCreator.get().create(pipelineConfiguration, errors, null);
    Utils.checkArgument(config != null, Utils.formatL("Invalid pipeline configuration: {}", errors));
    String numExecutors = config.workerCount == 0 ?
        sourceInfo.get(ClusterModeConstants.NUM_EXECUTORS_KEY) : String.valueOf(config.workerCount);
    List<String> args;
    File hostingDir = null;
    if (executionMode == ExecutionMode.CLUSTER_BATCH) {
      LOG.info("Submitting MapReduce Job");
      environment.put(CLUSTER_TYPE, CLUSTER_TYPE_MAPREDUCE);
      args = generateMRArgs(
          clusterManager.getAbsolutePath(),
          String.valueOf(config.clusterSlaveMemory),
          config.clusterSlaveJavaOpts,
          libsTarGz.getAbsolutePath(),
          etcTarGz.getAbsolutePath(),
          resourcesTarGz.getAbsolutePath(),
          log4jProperties.getAbsolutePath(),
          bootstrapJar.getAbsolutePath(),
          sdcPropertiesFile.getAbsolutePath(),
          clusterBootstrapJar.getAbsolutePath(),
          jarsToShip
      );
    } else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
      LOG.info("Submitting Spark Job on Yarn");
      environment.put(CLUSTER_TYPE, CLUSTER_TYPE_YARN);
      args = generateSparkArgs(
          clusterManager.getAbsolutePath(),
          String.valueOf(config.clusterSlaveMemory),
          config.clusterSlaveJavaOpts,
          config.sparkConfigs,
          numExecutors,
          libsTarGz.getAbsolutePath(),
          etcTarGz.getAbsolutePath(),
          resourcesTarGz.getAbsolutePath(),
          log4jProperties.getAbsolutePath(),
          bootstrapJar.getAbsolutePath(),
          jarsToShip,
          pipelineConfiguration.getTitle(),
          clusterBootstrapJar.getAbsolutePath()
      );
    } else if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      LOG.info("Submitting Spark Job on Mesos");
      environment.put(CLUSTER_TYPE, CLUSTER_TYPE_MESOS);
      environment.put(STAGING_DIR, stagingDir.getAbsolutePath());
      environment.put(MESOS_UBER_JAR_PATH, clusterBootstrapJar.getAbsolutePath());
      environment.put(MESOS_UBER_JAR, clusterBootstrapJar.getName());
      environment.put(ETC_TAR_ARCHIVE, "etc.tar.gz");
      environment.put(LIBS_TAR_ARCHIVE, "libs.tar.gz");
      environment.put(RESOURCES_TAR_ARCHIVE, "resources.tar.gz");
      hostingDir = new File(runtimeInfo.getDataDir(), Utils.checkNotNull(mesosHostingJarDir, "mesos jar dir cannot be null"));
      if (!hostingDir.mkdirs()) {
        throw new RuntimeException("Couldn't create hosting dir: " + hostingDir.toString());
      }
      environment.put(MESOS_HOSTING_JAR_DIR, hostingDir.getAbsolutePath());
      args =
        generateMesosArgs(clusterManager.getAbsolutePath(), config.mesosDispatcherURL,
          Utils.checkNotNull(mesosURL, "mesos jar url cannot be null"));
    } else {
      throw new IllegalStateException(Utils.format("Incorrect execution mode: {}", executionMode));
    }
    SystemProcess process = systemProcessFactory.create(ClusterProviderImpl.class.getSimpleName(), outputDir, args);
    LOG.info("Starting: " + process);
    try {
      process.start(environment);
      long start = System.currentTimeMillis();
      Set<String> applicationIds = new HashSet<>();
      while (true) {
        long elapsedSeconds = TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        LOG.debug("Waiting for application id, elapsed seconds: " + elapsedSeconds);
        if (applicationIds.size() > 1) {
          logOutput("unknown", process);
          throw new IllegalStateException(errorString("Found more than one application id: {}", applicationIds));
        } else if (!applicationIds.isEmpty()) {
          String appId = applicationIds.iterator().next();
          logOutput(appId, process);
          ApplicationState applicationState = new ApplicationState();
          applicationState.setId(appId);
          applicationState.setSdcToken(clusterToken);
          if (mesosHostingJarDir != null) {
            applicationState.setDirId(mesosHostingJarDir);
          }
          return applicationState;
        }
        if (!ThreadUtil.sleep(1000)) {
          if (hostingDir != null) {
            FileUtils.deleteQuietly(hostingDir);
          }
          throw new IllegalStateException("Interrupted while waiting for pipeline to start");
        }
        List<String> lines = new ArrayList<>();
        lines.addAll(process.getOutput());
        lines.addAll(process.getError());
        Matcher m;
        for (String line : lines) {
          if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
            m = MESOS_DRIVER_ID_REGEX.matcher(line);
          } else {
            m = YARN_APPLICATION_ID_REGEX.matcher(line);
          }
          if (m.find()) {
            LOG.info("Found application id " + m.group(1));
            applicationIds.add(m.group(1));
          }
          m = NO_VALID_CREDENTIALS.matcher(line);
          if (m.find()) {
            LOG.info("Kerberos Error found on line: " + line);
            String msg = "Kerberos Error: " + m.group(1);
            throw new IOException(msg);
          }
        }
        if (elapsedSeconds > timeToWaitForFailure) {
          logOutput("unknown", process);
          String msg = Utils.format("Timed out after waiting {} seconds for for cluster application to start. " +
              "Submit command {} alive.", elapsedSeconds, (process.isAlive() ? "is" : "is not"));
          if (hostingDir != null) {
            FileUtils.deleteQuietly(hostingDir);
          }
          throw new IllegalStateException(msg);
        }
      }
    } finally {
      process.cleanup();
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
        LOG.info(Utils.format("Adding {} to ship.", url.getPath()));
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
      LOG.info("Copying application token from absolute location {} to etc's staging dir: {}",
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
      LOG.debug("Datacollector library properties key : '{}', value: '{}'", key, value);
      if (key.equals(CLUSTER_BOOTSTRAP_JAR_REGEX + executionMode + "_" + stageDefinition.getClassName())) {
        LOG.info("Using bootstrap jar pattern: '{}'", value);
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

  private List<String> generateMesosArgs(String clusterManager, String mesosDispatcherURL, String mesosJar) {
    List<String> args = new ArrayList<>();
    args.add(clusterManager);
    args.add("start");
    args.add("--deploy-mode");
    args.add("cluster");
    // total executor cores option currently doesn't work for spark on mesos
    args.add("--total-executor-cores");
    args.add("1");
    args.add("--master");
    args.add(mesosDispatcherURL);
    args.add("--class");
    args.add("com.streamsets.pipeline.mesos.BootstrapMesosDriver");
    args.add(mesosJar);
    return args;
  }

  private List<String> generateMRArgs(String clusterManager, String slaveMemory, String javaOpts,
                                      String libsTarGz, String etcTarGz, String resourcesTarGz, String log4jProperties,
                                      String bootstrapJar, String sdcPropertiesFile,
                                      String clusterBootstrapJar, Set<String> jarsToShip) {
    List<String> args = new ArrayList<>();
    args.add(clusterManager);
    args.add("start");
    args.add("jar");
    args.add(clusterBootstrapJar);
    args.add("com.streamsets.pipeline.BootstrapClusterBatch");
    args.add("-archives");
    args.add(Joiner.on(",").join(libsTarGz, etcTarGz, resourcesTarGz));
    args.add("-D");
    args.add("mapreduce.job.log4j-properties-file=" + log4jProperties);
    args.add("-libjars");
    StringBuilder libJarString = new StringBuilder(bootstrapJar);
    for (String jarToShip : jarsToShip) {
      libJarString.append(",").append(jarToShip);
    }
    args.add(libJarString.toString());
    args.add(sdcPropertiesFile);
    args.add(
        Joiner.on(" ").join(
            String.format("-Xmx%sm", slaveMemory),
            javaOpts,
            "-javaagent:./" + (new File(bootstrapJar)).getName()
        )
    );
    return args;
  }

  private List<String> generateSparkArgs(
      String clusterManager,
      String slaveMemory,
      String javaOpts,
      Map<String, String> extraSparkConfigs,
      String numExecutors,
      String libsTarGz,
      String etcTarGz,
      String resourcesTarGz,
      String log4jProperties,
      String bootstrapJar,
      Set<String> jarsToShip,
      String pipelineTitle,
      String clusterBootstrapJar
  ) {
    List<String> args = new ArrayList<>();
    args.add(clusterManager);
    args.add("start");
    // we only support yarn-cluster mode
    args.add("--master");
    args.add("yarn");
    args.add("--deploy-mode");
    args.add("cluster");
    args.add("--executor-memory");
    args.add(slaveMemory + "m");
    // one single sdc per executor
    args.add("--executor-cores");
    args.add("1");

    // Number of Executors based on the origin parallelism
    checkNumExecutors(numExecutors);
    args.add("--num-executors");
    args.add(numExecutors);

    // ship our stage libs and etc directory
    args.add("--archives");
    args.add(Joiner.on(",").join(libsTarGz, etcTarGz, resourcesTarGz));
    // required or else we won't be able to log on cluster
    args.add("--files");
    args.add(log4jProperties);
    args.add("--jars");
    StringBuilder libJarString = new StringBuilder(bootstrapJar);
    for (String jarToShip : jarsToShip) {
      libJarString.append(",").append(jarToShip);
    }
    args.add(libJarString.toString());
    // use our javaagent and java opt configs
    args.add("--conf");
    args.add("spark.executor.extraJavaOptions=" +
        Joiner.on(" ").join("-javaagent:./" + (new File(bootstrapJar)).getName(), javaOpts)
    );
    extraSparkConfigs.forEach((k, v) -> {
      args.add("--conf");
      args.add(k + "=" + v);
    });
    // Job name in Resource Manager UI
    args.add("--name");
    args.add("StreamSets Data Collector: " + pipelineTitle);
    // main class
    args.add("--class");
    args.add("com.streamsets.pipeline.BootstrapClusterStreaming");
    args.add(clusterBootstrapJar);
    return args;
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
          LOG.debug("Adding to source configs " + confKey + "=" + confValue);
          sourceConfigs.put(confKey, confValue);
        }
      }
    }
  }

  private boolean canCastToString(Object value) {
    return value instanceof String || value instanceof Number || value.getClass().isPrimitive() ||
        value instanceof Boolean;
  }

  private void checkNumExecutors(String numExecutorsString) {
    Utils.checkNotNull(numExecutorsString, "Number of executors not found");
    int numExecutors;
    try {
      numExecutors = Integer.parseInt(numExecutorsString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Number of executors is not a valid integer");
    }
    Utils.checkArgument(numExecutors > 0, "Number of executors cannot be less than 1");
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
