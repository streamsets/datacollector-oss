/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryUtils;
import com.streamsets.pipeline.util.SystemProcess;
import com.streamsets.pipeline.util.SystemProcessFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkManager {
  private static final Logger LOG = LoggerFactory.getLogger(SparkManager.class);
  static final Pattern YARN_APPLICATION_ID_REGEX = Pattern.compile("\\s(application_[0-9]+_[0-9]+)\\s");
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final SystemProcessFactory systemProcessFactory;
  private final File tempDir;
  private final File sparkManager;
  private URLClassLoader apiCL;
  private URLClassLoader containerCL;

  public SparkManager(File tempDir) {
    this(new SystemProcessFactory(), tempDir, new File(new File(System.getProperty("user.dir"), "libexec"),
      "spark-manager"), null, null);
  }

  @VisibleForTesting
  public SparkManager(SystemProcessFactory systemProcessFactory, File tempDir, File sparkManager, URLClassLoader apiCL,
                      URLClassLoader containerCL) {
    this.systemProcessFactory = systemProcessFactory;
    this.tempDir = tempDir;
    this.sparkManager = sparkManager;
    if (containerCL == null) {
      this.containerCL = (URLClassLoader)getClass().getClassLoader();
    } else {
      this.containerCL = containerCL;
    }
    if (apiCL == null) {
      this.apiCL = (URLClassLoader)containerCL.getParent();
    } else {
      this.apiCL = apiCL;
    }
    Utils.checkState(sparkManager.isFile(), errorString("spark-manager does not exist: {}", sparkManager));
    Utils.checkState(sparkManager.canExecute(), errorString("spark-manager is not executable: {}", sparkManager));
  }

  public Future<ApplicationState> submit(final PipelineConfiguration pipelineConfiguration,
                                         final StageLibraryTask stageLibrary,
                                         final File etcDir,
                                         final Map<String, String> environment,
                                         final Map<String, String> sourceInfo,
                               final int timeoutInSecs) {
    return executorService.submit(new Callable<ApplicationState>() {
      @Override
      public ApplicationState call() throws Exception {
        ApplicationState state = new ApplicationState();
        state.setId(startPipeline(systemProcessFactory, sparkManager, tempDir, environment, sourceInfo,
          pipelineConfiguration, stageLibrary, etcDir, apiCL, containerCL, timeoutInSecs));
        return state;
      }
    });
  }

  public void kill(ApplicationState applicationState) throws TimeoutException {
    killPipeline(systemProcessFactory, sparkManager, tempDir, applicationState.getId());
  }

  public boolean isRunning(ApplicationState applicationState) throws TimeoutException {
    return isRunning(systemProcessFactory, sparkManager, tempDir, applicationState.getId());
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }

  private static void killPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                   String appId) throws TimeoutException {
    List<String> args = new ArrayList<>();
    args.add(sparkManager.getAbsolutePath());
    args.add("kill");
    args.add(appId);
    SystemProcess process = systemProcessFactory.create(SparkManager.class.getSimpleName(), tempDir, args);
    try {
      process.start();
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

  private static boolean isRunning(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                   String appId) throws TimeoutException {
    List<String> args = new ArrayList<>();
    args.add(sparkManager.getAbsolutePath());
    args.add("status");
    args.add(appId);
    SystemProcess process = systemProcessFactory.create(SparkManager.class.getSimpleName(), tempDir, args);
    try {
      process.start();
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

  private static void logOutput(String appId, SystemProcess process) {
    try {
      LOG.info("YARN status command standard error: {} ", Joiner.on("\n").join(process.getAllError()));
      LOG.info("YARN status command standard output: {} ", Joiner.on("\n").join(process.getAllOutput()));
    } catch (Exception e) {
      String msg = errorString("Could not read output of command '{}' for app {}: {}", process.getCommand(), appId, e);
      LOG.error(msg, e);
    }
  }



  private static String startPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                      Map<String, String> environment, Map<String, String> sourceInfo,
                                      PipelineConfiguration pipelineConfiguration, StageLibraryTask stageLibrary,
                                      final File etcDir, URLClassLoader apiCL, URLClassLoader containerCL,
                                      int timeoutInSecs) throws TimeoutException {
    File pipelineFile = new File(tempDir, "cluster-pipeline.json");
    try {
      ObjectMapperFactory.getOneLine().writeValue(pipelineFile, BeanHelper.
        wrapPipelineConfiguration(pipelineConfiguration));
    } catch (IOException e) {
      throw new RuntimeException(errorString("writing to temp file {} : {}", pipelineFile, e), e);
    }
    // timeToWaitForFailure is how long we wait to ensure the process was successfully
    // submitted to the cluster. For example it takes at least 20 seconds for a YARN
    // job to fail
    int timeToWaitForFailure = Math.max(1, (timeoutInSecs / 5));
    // create libs.tar.gz file for pipeline
    Map<String, URLClassLoader > streamsetsLibsCl = new HashMap<>();
    Map<String, URLClassLoader > userLibsCL = new HashMap<>();
    for (StageConfiguration stageConf : pipelineConfiguration.getStages()) {
      StageDefinition stageDef = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
        stageConf.getStageVersion());
      String type = StageLibraryUtils.getLibraryType(stageDef.getStageClassLoader());
      String name = StageLibraryUtils.getLibraryName(stageDef.getStageClassLoader());
      if (ClasspathConstants.STREAMSETS_LIBS.equals(type)) {
        streamsetsLibsCl.put(name, (URLClassLoader)stageDef.getStageClassLoader());
      } else if (ClasspathConstants.USER_LIBS.equals(type)) {
        userLibsCL.put(name, (URLClassLoader)stageDef.getStageClassLoader());
      } else {
        throw new IllegalStateException(Utils.format("Error unknown stage library type: {} ", type));
      }
    }
    File libsTarGz = new File(tempDir, "libs.tar.gz");
    try {
      TarFileCreator.createLibsTarGz(apiCL, containerCL, streamsetsLibsCl, userLibsCL, libsTarGz);
    } catch (Exception ex) {
      String msg = errorString("serializing classpath: {}", ex);
      throw new RuntimeException(msg, ex);
    }
    File etcTarGz = new File(tempDir, "etc.tar.gz");
    try {
      TarFileCreator.createEtcTarGz(etcDir, etcTarGz);
    } catch (Exception ex) {
      String msg = errorString("serializing etc directory: {}", ex);
      throw new RuntimeException(msg, ex);
    }
    List<String> args = new ArrayList<>();
    args.add(sparkManager.getAbsolutePath());
    args.add("start");
    args.add("--master");
    args.add("yarn-cluster");
    args.add("--archives");
    args.add(Joiner.on(",").join(libsTarGz.getAbsolutePath(), etcTarGz.getAbsolutePath()));
    args.add("--jars");
    args.add(new File(System.getProperty("user.dir"),
      "libexec/bootstrap-libs/main/streamsets-datacollector-bootstrap*.jar").getAbsolutePath());
    args.add("--conf");
    args.add("\"spark.executor.extraJavaOptions=-javaagent:./__app__.jar\"");
    args.add("--class");
    String mainClassName = "com.streamsets.pipeline.BootstrapMain";
    args.add(mainClassName);
    args.add(new File(System.getProperty("user.dir"),
      "libexec/bootstrap-libs/spark/streamsets-datacollector-spark-bootstrap*.jar").getAbsolutePath());
    SystemProcess process = systemProcessFactory.create(SparkManager.class.getSimpleName(), tempDir, args);
    LOG.info("Starting: " + process);
    try {
      try {
        process.start();
      } catch (IOException e) {
        String msg = errorString("Could not submit job: {}", e);
        throw new RuntimeException(msg, e);
      }
      long start = System.currentTimeMillis();
      Set<String> applicationIds = new HashSet<>();
      while (true) {
        long elapsedSeconds = TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        if (applicationIds.size() > 1) {
          logOutput("unknown", process);
          throw new IllegalStateException(errorString("Found more than one application id: {}", applicationIds));
        } else if (!applicationIds.isEmpty() && elapsedSeconds > timeToWaitForFailure) {
          String appId = applicationIds.iterator().next();
          logOutput(appId, process);
          return appId;
        } else if (elapsedSeconds > timeoutInSecs) {
          logOutput("unknown", process);
          throw new TimeoutException(errorString("Unable to find YARN application id after {} seconds", elapsedSeconds));
        }
        if (!ThreadUtil.sleep(1000)) {
          throw new IllegalStateException("Interrupted while waiting for pipeline to start");
        }
        for (String line : process.getOutput()) {
          Matcher m = YARN_APPLICATION_ID_REGEX.matcher(line);
          if (m.find()) {
            applicationIds.add(m.group(1));
          }
        }
      }
    } finally {
      process.cleanup();
    }
  }
}
