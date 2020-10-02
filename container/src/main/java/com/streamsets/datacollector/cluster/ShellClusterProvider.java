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
package com.streamsets.datacollector.cluster;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.security.SecurityConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.SystemProcessFactory;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.util.SystemProcess;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;

public class ShellClusterProvider extends BaseClusterProvider {
  private static final String KERBEROS_AUTH = "KERBEROS_AUTH";
  private static final String KERBEROS_KEYTAB = "KERBEROS_KEYTAB";
  private static final String KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";

  private static final String STAGING_DIR = "STAGING_DIR";
  private static final String MESOS_UBER_JAR_PATH = "MESOS_UBER_JAR_PATH";
  private static final String MESOS_UBER_JAR = "MESOS_UBER_JAR";
  private static final String ETC_TAR_ARCHIVE = "ETC_TAR_ARCHIVE";
  private static final String LIBS_TAR_ARCHIVE = "LIBS_TAR_ARCHIVE";
  private static final String RESOURCES_TAR_ARCHIVE = "RESOURCES_TAR_ARCHIVE";
  private static final String MESOS_HOSTING_JAR_DIR = "MESOS_HOSTING_JAR_DIR";

  private static final String MAPR_UNAME_PWD_SECURITY_ENABLED_KEY = "maprlogin.password.enabled";

  private final File clusterManagerScript;
  private StageLibraryTask stageLibraryTask;
  private final YARNStatusParser yarnStatusParser;
  private final MesosStatusParser mesosStatusParser;

  public ShellClusterProvider(
      RuntimeInfo runtimeInfo,
      @Nullable SecurityConfiguration securityConfiguration,
      Configuration conf,
      StageLibraryTask stageLibraryTask
  ) {
    super(runtimeInfo, securityConfiguration, conf, stageLibraryTask);
    clusterManagerScript = new File(runtimeInfo.getLibexecDir(), "_cluster-manager");
    Utils.checkState(
        clusterManagerScript.isFile(),
        errorString("_cluster-manager does not exist: {}", clusterManagerScript)
    );
    Utils.checkState(
        clusterManagerScript.canExecute(),
        errorString("_cluster-manager is not executable: {}", clusterManagerScript)
    );
    this.yarnStatusParser = new YARNStatusParser();
    this.mesosStatusParser = new MesosStatusParser();

  }

  protected SystemProcessFactory getSystemProcessFactory() {
    return new SystemProcessFactory();
  }

  protected File getClusterManagerScript() {
    return clusterManagerScript;
  }

  private void addKerberosConfiguration(Map<String, String> environment) {
    if (getSecurityConfiguration() != null) {
      environment.put(KERBEROS_AUTH, String.valueOf(getSecurityConfiguration().isKerberosEnabled()));
      if (getSecurityConfiguration().isKerberosEnabled()) {
        environment.put(KERBEROS_PRINCIPAL, getSecurityConfiguration().getKerberosPrincipal());
        environment.put(KERBEROS_KEYTAB, getSecurityConfiguration().getKerberosKeytab());
      }
    }
  }

  private void addProxyUserConfiguration(Map<String, String> environment, String user) {
    if (user != null) {
      getLog().info("Will submit MR job as user {}", user);
      environment.put(ClusterModeConstants.HADOOP_PROXY_USER, user);
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

  @Override
  public void killPipeline(
      File tempDir,
      ApplicationState applicationState,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  ) throws TimeoutException, IOException {
    String appId = applicationState.getAppId();
    Map<String, String> environment = new HashMap<>();
    environment.put(CLUSTER_TYPE, CLUSTER_TYPE_YARN);
    addKerberosConfiguration(environment);
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add(clusterManagerScript.getAbsolutePath());
    args.add("kill");
    args.add(appId);
    ExecutionMode executionMode = PipelineBeanCreator.get()
                                                     .getExecutionMode(pipelineConfiguration, new ArrayList<Issue>());
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      addMesosArgs(pipelineConfiguration, environment, args);
    }
    SystemProcess process = getSystemProcessFactory()
        .create(BaseClusterProvider.class.getSimpleName(), tempDir, args.build());
    try {
      process.start(environment);
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        throw new TimeoutException(errorString("Kill command for {} timed out.", appId));
      }
    } finally {
      process.cleanup();
    }
  }

  @Override
  public ClusterPipelineStatus getStatus(
      File tempDir,
      ApplicationState applicationState,
      PipelineConfiguration pipelineConfiguration, PipelineConfigBean pipelineConfigBean
  ) throws TimeoutException, IOException {
    String appId = applicationState.getAppId();
    Map<String, String> environment = new HashMap<>();
    environment.put(CLUSTER_TYPE, CLUSTER_TYPE_YARN);
    addKerberosConfiguration(environment);
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add(clusterManagerScript.getAbsolutePath());
    args.add("status");
    args.add(applicationState.getAppId());
    ExecutionMode executionMode = PipelineBeanCreator.get()
                                                     .getExecutionMode(pipelineConfiguration, new ArrayList<Issue>());
    if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      addMesosArgs(pipelineConfiguration, environment, args);
    }
    SystemProcess process = getSystemProcessFactory()
        .create(BaseClusterProvider.class.getSimpleName(), tempDir, args.build());
    try {
      process.start(environment);
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        throw new TimeoutException(errorString("YARN status command for {} timed out.", applicationState.getAppId()));
      }
      if (process.exitValue() != 0) {
        throw new IllegalStateException(errorString("Status command for {} failed with exit code {}.",
            applicationState.getAppId(),
            process.exitValue()));
      }
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

  @Override
  public void cleanUp(
      ApplicationState applicationState,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean
  ) throws IOException {
    // NO-OP
  }

  @Override
  protected ApplicationState startPipelineExecute(
      File outputDir,
      Map<String, String> sourceInfo,
      PipelineConfiguration pipelineConfiguration,
      PipelineConfigBean pipelineConfigBean,
      long timeToWaitForFailure,
      File stagingDir,
      String clusterToken,
      File clusterBootstrapJar,
      File bootstrapJar, Set<String> jarsToShip,
      File libsTarGz,
      File resourcesTarGz,
      File etcTarGz,
      File sdcPropertiesFile,
      File log4jProperties,
      String mesosHostingJarDir,
      String mesosURL,
      String clusterBootstrapApiJar,
      String user,
      List<Issue> errors
  ) throws IOException {
    ExecutionMode executionMode = PipelineBeanCreator.get().getExecutionMode(pipelineConfiguration, new ArrayList<>());
    Map<String, String> environment = new HashMap<>(pipelineConfigBean.clusterLauncherEnv);
    addKerberosConfiguration(environment);
    errors.clear();
    PipelineConfigBean config = PipelineBeanCreator.get().create(pipelineConfiguration, errors, null, user, new HashMap<>());
    Utils.checkArgument(config != null, Utils.formatL("Invalid pipeline configuration: {}", errors));
    String numExecutors = config.workerCount == 0 ?
                          sourceInfo.get(ClusterModeConstants.NUM_EXECUTORS_KEY) : String.valueOf(config.workerCount);

    String securityProtocol = sourceInfo.get(ClusterModeConstants.EXTRA_KAFKA_CONFIG_PREFIX + "security.protocol");
    boolean secureKafka = false;
    if (securityProtocol != null && securityProtocol.toUpperCase().contains(ClusterModeConstants
        .SECURE_KAFKA_IDENTIFIER)) {
      secureKafka = true;
    }

    List<String> args;
    File hostingDir = null;
    String slaveJavaOpts = config.clusterSlaveJavaOpts + Optional.ofNullable(System.getProperty(MAPR_UNAME_PWD_SECURITY_ENABLED_KEY))
                                                                 .map(opValue -> !config.clusterSlaveJavaOpts.contains(MAPR_UNAME_PWD_SECURITY_ENABLED_KEY)? " -D" + MAPR_UNAME_PWD_SECURITY_ENABLED_KEY + "=" + opValue : "")
                                                                 .orElse("");
    getLog().info("Slave Java Opts : {}", slaveJavaOpts);
    if (executionMode == ExecutionMode.CLUSTER_BATCH) {
      addProxyUserConfiguration(environment, sourceInfo.get(ClusterModeConstants.HADOOP_PROXY_USER));
      getLog().info("Submitting MapReduce Job");
      environment.put(CLUSTER_TYPE, CLUSTER_TYPE_MAPREDUCE);
      args = generateMRArgs(
          getClusterManagerScript().getAbsolutePath(),
          String.valueOf(config.clusterSlaveMemory),
          slaveJavaOpts,
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
      getLog().info("Submitting Spark Job on Yarn");
      environment.put(CLUSTER_TYPE, CLUSTER_TYPE_YARN);
      args = generateSparkArgs(
          getClusterManagerScript().getAbsolutePath(),
          String.valueOf(config.clusterSlaveMemory),
          slaveJavaOpts,
          config.sparkConfigs,
          numExecutors,
          libsTarGz.getAbsolutePath(),
          etcTarGz.getAbsolutePath(),
          resourcesTarGz.getAbsolutePath(),
          log4jProperties.getAbsolutePath(),
          bootstrapJar.getAbsolutePath(),
          jarsToShip,
          pipelineConfiguration.getTitle(),
          clusterBootstrapApiJar,
          secureKafka
      );
    } else if (executionMode == ExecutionMode.CLUSTER_MESOS_STREAMING) {
      getLog().info("Submitting Spark Job on Mesos");
      environment.put(CLUSTER_TYPE, CLUSTER_TYPE_MESOS);
      environment.put(STAGING_DIR, stagingDir.getAbsolutePath());
      environment.put(MESOS_UBER_JAR_PATH, clusterBootstrapJar.getAbsolutePath());
      environment.put(MESOS_UBER_JAR, clusterBootstrapJar.getName());
      environment.put(ETC_TAR_ARCHIVE, "etc.tar.gz");
      environment.put(LIBS_TAR_ARCHIVE, "libs.tar.gz");
      environment.put(RESOURCES_TAR_ARCHIVE, "resources.tar.gz");
      hostingDir = new File(getRuntimeInfo().getDataDir(), Utils.checkNotNull(mesosHostingJarDir, "mesos jar dir cannot be null"));
      if (!hostingDir.mkdirs()) {
        throw new RuntimeException("Couldn't create hosting dir: " + hostingDir.toString());
      }
      environment.put(MESOS_HOSTING_JAR_DIR, hostingDir.getAbsolutePath());
      args =
          generateMesosArgs(
              getClusterManagerScript().getAbsolutePath(), config.mesosDispatcherURL,
              Utils.checkNotNull(mesosURL, "mesos jar url cannot be null"));
    } else {
      throw new IllegalStateException(Utils.format("Incorrect execution mode: {}", executionMode));
    }
    SystemProcess process = getSystemProcessFactory().create(BaseClusterProvider.class.getSimpleName(), outputDir, args);
    getLog().info("Starting: " + process);
    try {
      process.start(environment);
      long start = System.currentTimeMillis();
      Set<String> applicationIds = new HashSet<>();
      while (true) {
        long elapsedSeconds = TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        getLog().debug("Waiting for application id, elapsed seconds: " + elapsedSeconds);
        if (applicationIds.size() > 1) {
          throw new IllegalStateException(errorString("Found more than one application id: {}", applicationIds));
        } else if (!applicationIds.isEmpty()) {
          String appId = applicationIds.iterator().next();
          ApplicationState applicationState = new ApplicationState();
          applicationState.setAppId(appId);
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
            getLog().info("Found application id " + m.group(1));
            applicationIds.add(m.group(1));
          }
          m = NO_VALID_CREDENTIALS.matcher(line);
          if (m.find()) {
            getLog().info("Kerberos Error found on line: " + line);
            String msg = "Kerberos Error: " + m.group(1);
            throw new IOException(msg);
          }
        }
        if (elapsedSeconds > timeToWaitForFailure) {
          String msg = Utils.format("Timed out after waiting {} seconds for for cluster application to start. " +
              "Submit command {} alive.", elapsedSeconds, (process.isAlive() ? "is" : "is not"));
          if (hostingDir != null) {
            FileUtils.deleteQuietly(hostingDir);
          }
          Iterator<String> idIterator = applicationIds.iterator();
          if (idIterator.hasNext()) {
            String appId = idIterator.next();
            SystemProcess logsProcess = getSystemProcessFactory().create(
                "yarn",
                com.google.common.io.Files.createTempDir(),
                Arrays.asList("logs", "-applicationId", appId)
            );
          }
          throw new IllegalStateException(msg);
        }
      }
    } finally {
      process.cleanup();
    }
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
      String clusterBootstrapJar,
      boolean secureKafka
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

    // Add Security options
    if (getSecurityConfiguration() != null && getSecurityConfiguration().isKerberosEnabled()) {
      args.add("--keytab");
      args.add(getSecurityConfiguration().getKerberosKeytab());
      args.add("--principal");
      args.add(getSecurityConfiguration().getKerberosPrincipal());
    }

    if (secureKafka) {
      String jaasPath = System.getProperty(WebServerTask.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
      String loginConf = "-Djava.security.auth.login.config";
      args.add("--conf");
      args.add(Joiner.on("=").join("spark.driver.extraJavaOptions", loginConf, jaasPath));
      javaOpts = Utils.format("{} {}={}", javaOpts, loginConf, jaasPath);
    }
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

}
