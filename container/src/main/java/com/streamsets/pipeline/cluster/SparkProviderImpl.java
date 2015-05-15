/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineDefConfigs;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.http.WebServerTask;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryUtils;
import com.streamsets.pipeline.util.SystemProcess;
import com.streamsets.pipeline.util.SystemProcessFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkProviderImpl implements SparkProvider {
  private static final String RUN_FROM_SDC = "RUN_FROM_SDC";
  private static final String KERBEROS_AUTH = "KERBEROS_AUTH";
  private static final String KERBEROS_KEYTAB = "KERBEROS_KEYTAB";
  private static final String KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";
  static final Pattern YARN_APPLICATION_ID_REGEX = Pattern.compile("\\s(application_[0-9]+_[0-9]+)\\s");
  private final RuntimeInfo runtimeInfo;

  private static final Logger LOG = LoggerFactory.getLogger(SparkProviderImpl.class);

  @VisibleForTesting
  SparkProviderImpl() {
    this(null);
  }
  public SparkProviderImpl(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void killPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                    String appId, PipelineConfiguration pipelineConfiguration) throws TimeoutException {
    Map<String, String> environment = new HashMap<>();
    environment.put(RUN_FROM_SDC, Boolean.TRUE.toString());
    addKerberosConfiguration(environment, pipelineConfiguration);
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add(sparkManager.getAbsolutePath());
    args.add("kill");
    args.add(appId);
    SystemProcess process = systemProcessFactory.create(SparkManager.class.getSimpleName(), tempDir, args.build());
    try {
      process.start(environment);
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        logOutput(appId, process);
        throw new TimeoutException(errorString("YARN kill command for {} timed out.", appId));
      }
    } catch (IOException e) {
      String msg = errorString("Could not kill job: {}", e);
      throw new RuntimeException(msg, e);
    } catch (InterruptedException e) {
      String msg = errorString("Could not kill job: {}", e);
      throw new RuntimeException(msg, e);
    } finally {
      process.cleanup();
    }
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }

  private static void logOutput(String appId, SystemProcess process) {
    try {
      // TODO fix bug where calling this is required before getAll* works
      process.getError();
      process.getOutput();
      LOG.info("YARN status command standard error: {} ", Joiner.on("\n").join(process.getAllError()));
      LOG.info("YARN status command standard output: {} ", Joiner.on("\n").join(process.getAllOutput()));
    } catch (Exception e) {
      String msg = errorString("Could not read output of command '{}' for app {}: {}", process.getCommand(), appId, e);
      LOG.error(msg, e);
    }
  }

  @Override
  public boolean isRunning(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                    String appId, PipelineConfiguration pipelineConfiguration) throws TimeoutException {
    Map<String, String> environment = new HashMap<>();
    environment.put(RUN_FROM_SDC, Boolean.TRUE.toString());
    addKerberosConfiguration(environment, pipelineConfiguration);
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add(sparkManager.getAbsolutePath());
    args.add("status");
    args.add(appId);
    SystemProcess process = systemProcessFactory.create(SparkManager.class.getSimpleName(), tempDir, args.build());
    try {
      process.start(environment);
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        logOutput(appId, process);
        throw new TimeoutException(errorString("YARN status command for {} timed out.", appId));
      }
      if (process.exitValue() != 0) {
        logOutput(appId, process);
        throw new IllegalStateException(errorString("YARN status command for {} failed with exit code {}.", appId,
          process.exitValue()));
      }
      logOutput(appId, process);
      for (String line : process.getOutput()) {
        if (line.trim().equals("RUNNING")) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      String msg = errorString("Could not get job status: {}", e);
      throw new RuntimeException(msg, e);
    } catch (InterruptedException e) {
      String msg = errorString("Could not get job status: {}", e);
      throw new RuntimeException(msg, e);
    } finally {
      process.cleanup();
    }
  }

  private void rewriteProperties(File sdcPropertiesFile, Map<String, String> sourceConfigs) throws IOException{
    InputStream sdcInStream = null;
    OutputStream sdcOutStream = null;
    Properties sdcProperties = new Properties();
    try {
      sdcInStream = new FileInputStream(sdcPropertiesFile);
      sdcProperties.load(sdcInStream);
      sdcProperties.setProperty(WebServerTask.HTTP_PORT_KEY, "0");
      sdcProperties.setProperty(RuntimeModule.SDC_EXECUTION_MODE_KEY,
        RuntimeInfo.ExecutionMode.SLAVE.name().toLowerCase());

      if(runtimeInfo != null) {
        sdcProperties.setProperty(PipelineManager.SDC_CLUSTER_TOKEN_KEY, runtimeInfo.getSDCToken());
        sdcProperties.setProperty(PipelineManager.CALLBACK_SERVER_URL_KEY, runtimeInfo.getClusterCallbackURL());
      }

      for (Map.Entry<String, String> entry : sourceConfigs.entrySet()) {
        sdcProperties.setProperty(entry.getKey(), entry.getValue());
      }
      sdcOutStream = new FileOutputStream(sdcPropertiesFile);
      sdcProperties.store(sdcOutStream, null);
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

  private static File getBootstrapJar(File bootstrapDir, final String name) {
    Utils.checkState(bootstrapDir.isDirectory(), Utils.format("SDC bootstrap lib does not exist: {}", bootstrapDir));
    File[] canidates = bootstrapDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File canidate) {
        return canidate.getName().startsWith(name) &&
          canidate.getName().endsWith(".jar");
      }
    });
    Utils.checkState(canidates != null, Utils.format("Did not find jar matching {} in {}", name, bootstrapDir));
    Utils.checkState(canidates.length == 1, Utils.format("Did not find exactly one bootstrap jar: {}",
      Arrays.toString(canidates)));
    return canidates[0];
  }

  private void addKerberosConfiguration(Map<String, String> environment, PipelineConfiguration pipelineConfiguration) {
    ConfigConfiguration clusterKerberos = Utils.checkNotNull(pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      CLUSTER_KERBEROS_AUTH_CONFIG), "Could not obtain cluster kerberos auth");
    ConfigConfiguration kerberosPrincipal = Utils.checkNotNull(pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      CLUSTER_KERBEROS_PRINCIPAL_CONFIG), "Could not obtain cluster kerberos principal");
    ConfigConfiguration kerberosKeytab = Utils.checkNotNull(pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      CLUSTER_KERBEROS_KEYTAB_CONFIG), "Could not obtain cluster kerberos keytab");
    environment.put(KERBEROS_AUTH, String.valueOf(clusterKerberos.getValue()));
    if ((Boolean)clusterKerberos.getValue()) {
      environment.put(KERBEROS_PRINCIPAL, String.valueOf(kerberosPrincipal.getValue()));
      environment.put(KERBEROS_KEYTAB, String.valueOf(kerberosKeytab.getValue()));
    }
  }

  @Override
  public ApplicationState startPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                       Map<String, String> environment, Map<String, String> sourceInfo,
                       PipelineConfiguration pipelineConfiguration, StageLibraryTask stageLibrary,
                       File etcDir, File staticWebDir, File bootstrapDir, URLClassLoader apiCL,
                       URLClassLoader containerCL, long timeToWaitForFailure) throws TimeoutException {
    environment = Maps.newHashMap(environment);
    environment.put(RUN_FROM_SDC, Boolean.TRUE.toString());
    // create libs.tar.gz file for pipeline
    Map<String, URLClassLoader > streamsetsLibsCl = new HashMap<>();
    Map<String, URLClassLoader > userLibsCL = new HashMap<>();
    Map<String, String> sourceConfigs = new HashMap<>();
    ImmutableList.Builder<StageConfiguration> stageConfigurations = ImmutableList.builder();
    stageConfigurations.addAll(pipelineConfiguration.getStages());
    stageConfigurations.add(pipelineConfiguration.getErrorStage());
    String sdcClusterToken = UUID.randomUUID().toString();
    if (runtimeInfo != null) {
      runtimeInfo.setSDCToken(sdcClusterToken);
    }
    String pathToSparkKafkaJar = null;
    for (StageConfiguration stageConf : stageConfigurations.build()) {
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
        stageConf.getStageVersion());
      if (stageConf.getInputLanes().isEmpty()) {
        for (ConfigConfiguration conf : stageConf.getConfiguration()) {
          if (conf.getValue() != null) {
            if (canCastToString(conf.getValue())) {
              LOG.debug("Adding to source configs " + conf.getName() + "=" + conf.getValue());
              sourceConfigs.put(conf.getName(), String.valueOf(conf.getValue()));
            } else if (conf.getValue() instanceof List) {
              List<Map<String, Object>> arrayListValues = (List<Map<String, Object>>) conf.getValue();
              if (!arrayListValues.isEmpty()) {
                addToSourceConfigs(sourceConfigs, arrayListValues);
              } else {
                LOG.debug("Conf value for " + conf.getName() + " is empty");
              }
            } else {
              LOG.warn("Conf value is of unknown type " + conf.getValue());
            }
          }
        }
        // find the spark-kafka jar
        for (URL jarUrl : ((URLClassLoader)stageDef.getStageClassLoader()).getURLs()) {
          File jarFile = new File(jarUrl.getPath());
          if (jarFile.getName().startsWith(ClusterModeConstants.SPARK_KAFKA_JAR_PREFIX)) {
            pathToSparkKafkaJar = jarFile.getAbsolutePath();
          }
        }
      }
      String type = StageLibraryUtils.getLibraryType(stageDef.getStageClassLoader());
      String name = StageLibraryUtils.getLibraryName(stageDef.getStageClassLoader());
      if (ClusterModeConstants.STREAMSETS_LIBS.equals(type)) {
        streamsetsLibsCl.put(name, (URLClassLoader)stageDef.getStageClassLoader());
      } else if (ClusterModeConstants.USER_LIBS.equals(type)) {
        userLibsCL.put(name, (URLClassLoader)stageDef.getStageClassLoader());
      } else {
        throw new IllegalStateException(Utils.format("Error unknown stage library type: {} ", type));
      }
    }
    Utils.checkState(pathToSparkKafkaJar != null, "Could not find spark kafka jar");
    Utils.checkState(staticWebDir.isDirectory(), Utils.format("Expected {} to be a directory", staticWebDir));
    File libsTarGz = new File(tempDir, "libs.tar.gz");
    try {
      TarFileCreator.createLibsTarGz(apiCL, containerCL, streamsetsLibsCl, userLibsCL,
        ClusterModeConstants.EXCLUDED_JAR_PREFIXES, staticWebDir, libsTarGz);
    } catch (Exception ex) {
      String msg = errorString("serializing classpath: {}", ex);
      throw new RuntimeException(msg, ex);
    }
    File etcTarGz = new File(tempDir, "etc.tar.gz");
    try {
      File tempEtcDir = new File(tempDir, "etc");
      FileUtils.deleteQuietly(tempEtcDir);
      Utils.checkState(tempEtcDir.mkdir(), Utils.formatL("Could not create {}", tempEtcDir));
      FileUtils.copyDirectory(etcDir, tempEtcDir);
      etcDir = tempEtcDir;
      File pipelineFile = new File(etcDir, "pipeline.json");
      ObjectMapperFactory.getOneLine().writeValue(pipelineFile, BeanHelper.
        wrapPipelineConfiguration(pipelineConfiguration));
      File sdcPropertiesFile = new File(etcDir, "sdc.properties");

      rewriteProperties(sdcPropertiesFile, sourceConfigs);
      TarFileCreator.createEtcTarGz(etcDir, etcTarGz);
    } catch (Exception ex) {
      String msg = errorString("serializing etc directory: {}", ex);
      throw new RuntimeException(msg, ex);
    }
    File bootstrapJar = getBootstrapJar(new File(bootstrapDir, "main"), "streamsets-datacollector-bootstrap");
    File sparkBootstrapJar = getBootstrapJar(new File(bootstrapDir, "spark"),
      "streamsets-datacollector-spark-bootstrap");
    File log4jProperties = new File(tempDir, "log4j.properties");
    InputStream clusterLog4jProperties = null;
    try {
      clusterLog4jProperties = Utils.checkNotNull(getClass().getResourceAsStream("/cluster-log4j.properties"),
        "Cluster Log4J Properties");
      FileUtils.copyInputStreamToFile(clusterLog4jProperties, log4jProperties);
    } catch (IOException ex) {
      String msg = errorString("copying log4j configuration: {}", ex);
      throw new RuntimeException(msg, ex);
    } finally {
      if (clusterLog4jProperties != null) {
        IOUtils.closeQuietly(clusterLog4jProperties);
      }
    }
    addKerberosConfiguration(environment, pipelineConfiguration);
    List<String> args = new ArrayList<>();
    args.add(sparkManager.getAbsolutePath());
    args.add("start");
    // we only support yarn-cluster mode
    args.add("--master");
    args.add("yarn-cluster");
    ConfigConfiguration slaveMemory = Utils.checkNotNull(pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      CLUSTER_SLAVE_MEMORY_CONFIG), "Could not obtain cluster slave memory");
    args.add("--executor-memory");
    args.add(slaveMemory.getValue() + "m");
    // one single sdc per executor
    args.add("--executor-cores");
    args.add("1");

    // Number of Executors based on the origin parallelism
    String numExecutors = sourceInfo.get(ClusterModeConstants.NUM_EXECUTORS_KEY);
    checkNumExecutors(numExecutors);
    args.add("--num-executors");
    args.add(numExecutors);

    // ship our stage libs and etc directory
    args.add("--archives");
    args.add(Joiner.on(",").join(libsTarGz.getAbsolutePath(), etcTarGz.getAbsolutePath()));
    // required or else we won't be able to log on cluster
    args.add("--files");
    args.add(log4jProperties.getAbsolutePath());
    args.add("--jars");
    args.add(Joiner.on(",").join(bootstrapJar.getAbsoluteFile(), pathToSparkKafkaJar));
    ConfigConfiguration javaOpts = Utils.checkNotNull(pipelineConfiguration.getConfiguration(PipelineDefConfigs.
      CLUSTER_SLAVE_JAVA_OPTS_CONFIG), "Could not obtain cluster worker java options");
    // use our javaagent and java opt configs
    args.add("--conf");
    args.add("spark.executor.extraJavaOptions=" + Joiner.on(" ").join("-javaagent:./" + bootstrapJar.getName(),
      javaOpts.getValue()));
    // main class
    args.add("--class");
    args.add("com.streamsets.pipeline.BootstrapCluster");
    args.add(sparkBootstrapJar.getAbsolutePath());
    SystemProcess process = systemProcessFactory.create(SparkManager.class.getSimpleName(), tempDir, args);
    LOG.info("Starting: " + process);
    try {
      try {
        process.start(environment);
      } catch (IOException e) {
        String msg = errorString("Could not submit job: {}", e);
        throw new RuntimeException(msg, e);
      }
      long start = System.currentTimeMillis();
      Set<String> applicationIds = new HashSet<>();
      while (true) {
        long elapsedSeconds = TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        LOG.debug("Elapsed seconds till an application id is found are " + elapsedSeconds);
        if (applicationIds.size() > 1) {
          logOutput("unknown", process);
          throw new IllegalStateException(errorString("Found more than one application id: {}", applicationIds));
        } else if (!applicationIds.isEmpty()) {
          String appId = applicationIds.iterator().next();
          logOutput(appId, process);
          ApplicationState applicationState = new ApplicationState();
          applicationState.setId(appId);
          applicationState.setSdcToken(sdcClusterToken);
          return applicationState;
        }
        if (!ThreadUtil.sleep(1000)) {
          throw new IllegalStateException("Interrupted while waiting for pipeline to start");
        }
        for (String line : process.getOutput()) {
          Matcher m = YARN_APPLICATION_ID_REGEX.matcher(line);
          if (m.find()) {
            LOG.info("Found application id " + m.group(1));
            applicationIds.add(m.group(1));
          }
        }
        if (elapsedSeconds > timeToWaitForFailure) {
          if (process.isAlive()) {
            throw new IllegalStateException(
              "Exiting before the pipeline can start as the process submitting the spark job is running "
                + "for more than" + elapsedSeconds);
          } else {
            throw new IllegalStateException("Process submitting the spark job is dead and "
              + "couldn't retrieve the YARN application id");
          }
        }
      }
    } finally {
      process.cleanup();
    }
  }

  private void addToSourceConfigs(Map<String, String> sourceConfigs, List<Map<String, Object>> arrayListValues) {
    for (Map<String, Object> map : arrayListValues) {
      String confKey = null;
      String confValue = null;
      for (Map.Entry<String, Object> mapEntry : map.entrySet()) {
        String mapKey = mapEntry.getKey();
        Object mapValue = mapEntry.getValue();
        if (mapKey.equals("key")) {
          // Assuming the key is always string
          confKey = String.valueOf(mapValue);
        } else if (mapKey.equals("value")) {
          confValue = canCastToString(mapValue) ? String.valueOf(mapValue) : null;
        } else {
          confKey = mapKey;
          confValue = canCastToString(mapValue) ? String.valueOf(mapValue) : null;
        }
        if (confKey != null && confValue != null) {
          LOG.debug("Adding to source configs " + confKey + "=" + confValue);
          sourceConfigs.put(confKey, confValue);
        }
      }
    }
  }

  private boolean canCastToString(Object value) {
    if (value instanceof String || value instanceof Number || value.getClass().isPrimitive()
      || value instanceof Boolean) {
      return true;
    }
    return false;
  }

  private void checkNumExecutors(String numExecutorsString) {
    Utils.checkNotNull(numExecutorsString,"Number of executors not found");
    int numExecutors;
    try {
      numExecutors = Integer.parseInt(numExecutorsString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Number of executors is not a valid integer");
    }
    Utils.checkArgument(numExecutors > 0, "Number of executors cannot be less than 1");
  }
}
