/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import com.streamsets.pipeline.util.SystemProcessImpl;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkManager {
  private static final Logger LOG = LoggerFactory.getLogger(SparkManager.class);
  private final ListeningExecutorService executorService = MoreExecutors.
    listeningDecorator(Executors.newFixedThreadPool(3));
  private final SystemProcessFactory systemProcessFactory;
  private final SparkProvider sparkProvider;
  private final File tempDir;
  private final File sparkManager;
  private final int timeToWaitForFailure;
  private URLClassLoader apiCL;
  private URLClassLoader containerCL;

  public SparkManager(File tempDir) {
    this(new SystemProcessFactory(), new SparkProviderImpl(), tempDir,
      new File(new File(System.getProperty("user.dir"), "libexec"), "spark-manager"), null, null, 180);
  }

  @VisibleForTesting
  public SparkManager(SystemProcessFactory systemProcessFactory, SparkProvider sparkProvider,
                      File tempDir, File sparkManager, URLClassLoader apiCL,
                      URLClassLoader containerCL, int timeToWaitForFailure) {
    this.systemProcessFactory = systemProcessFactory;
    this.sparkProvider = sparkProvider;
    this.tempDir = tempDir;
    this.sparkManager = sparkManager;
    if (containerCL == null) {
      this.containerCL = (URLClassLoader)getClass().getClassLoader();
    } else {
      this.containerCL = containerCL;
    }
    if (apiCL == null) {
      this.apiCL = (URLClassLoader)this.containerCL.getParent();
    } else {
      this.apiCL = apiCL;
    }
    this.timeToWaitForFailure = timeToWaitForFailure;
    Utils.checkState(tempDir.isDirectory(), errorString("Temp directory does not exist: {}", tempDir));
    Utils.checkState(sparkManager.isFile(), errorString("spark-manager does not exist: {}", sparkManager));
    Utils.checkState(sparkManager.canExecute(), errorString("spark-manager is not executable: {}", sparkManager));
  }

  public ListenableFuture<ApplicationState> submit(final PipelineConfiguration pipelineConfiguration,
                                         final StageLibraryTask stageLibrary,
                                         final File etcDir, final File staticWebDir, final File bootstrapDir,
                                         final Map<String, String> environment,
                                         final Map<String, String> sourceInfo) {
    return executorService.submit(new Callable<ApplicationState>() {
      @Override
      public ApplicationState call() throws Exception {
        ApplicationState state = new ApplicationState();
        state.setId(sparkProvider.startPipeline(systemProcessFactory, sparkManager, tempDir, environment, sourceInfo,
          pipelineConfiguration, stageLibrary, etcDir, staticWebDir, bootstrapDir, apiCL, containerCL,
          timeToWaitForFailure));
        return state;
      }
    });
  }

  public ListenableFuture<Void> kill(final ApplicationState applicationState) {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        sparkProvider.killPipeline(systemProcessFactory, sparkManager, tempDir, applicationState.getId());
        return null;
      }
    });
  }

  public ListenableFuture<Boolean> isRunning(final ApplicationState applicationState) {
    return executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return sparkProvider.isRunning(systemProcessFactory, sparkManager, tempDir, applicationState.getId());
      }
    });
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }
}
