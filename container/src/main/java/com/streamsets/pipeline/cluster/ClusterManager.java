/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.SystemProcessFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

//TODO - Remove after refactoring  - To be replaced by ClusterManager in dataCollector.execution package
public class ClusterManager {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
  private final ListeningExecutorService executorService = MoreExecutors.
    listeningDecorator(Executors.newFixedThreadPool(3));
  private final SystemProcessFactory systemProcessFactory;
  private final ClusterProvider clusterProvider;
  private final File tempDir;
  private final File clusterManagerFile;
  private URLClassLoader apiCL;
  private URLClassLoader containerCL;

  public ClusterManager(RuntimeInfo runtimeInfo, File tempDir) {
    this(new SystemProcessFactory(), new ClusterProviderImpl(runtimeInfo), tempDir,
      new File(runtimeInfo.getLibexecDir(), "_cluster-manager"), null, null);
  }

  @VisibleForTesting
  public ClusterManager(SystemProcessFactory systemProcessFactory, ClusterProvider clusterProvider,
                        File tempDir, File clusterManagerFile, URLClassLoader apiCL,
                        URLClassLoader containerCL) {
    this.systemProcessFactory = systemProcessFactory;
    this.clusterProvider = clusterProvider;
    this.tempDir = tempDir;
    this.clusterManagerFile = clusterManagerFile;
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
    Utils.checkState(tempDir.isDirectory(), errorString("Temp directory does not exist: {}", tempDir));
    Utils.checkState(clusterManagerFile.isFile(), errorString("_cluster-manager does not exist: {}", clusterManagerFile));
    Utils.checkState(clusterManagerFile.canExecute(), errorString("_cluster-manager is not executable: {}", clusterManagerFile));
  }

  public ListenableFuture<ApplicationState> submit(final PipelineConfiguration pipelineConfiguration,
                                         final StageLibraryTask stageLibrary,
                                         final File etcDir, final File resourcesDir, final File staticWebDir,
                                         final File bootstrapDir, final Map<String, String> environment,
                                         final Map<String, String> sourceInfo, final long timeout)
                                         throws TimeoutException, IOException {
    return executorService.submit(new Callable<ApplicationState>() {
      @Override
      public ApplicationState call() throws Exception {
        return clusterProvider.startPipeline(systemProcessFactory, clusterManagerFile, tempDir, environment, sourceInfo,
          pipelineConfiguration, stageLibrary, etcDir, resourcesDir, staticWebDir, bootstrapDir, apiCL, containerCL,
          timeout);
      }
    });
  }

  public ListenableFuture<Void> kill(final ApplicationState applicationState,
                                     final PipelineConfiguration pipelineConfiguration) throws TimeoutException, IOException {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        clusterProvider.killPipeline(systemProcessFactory, clusterManagerFile, tempDir, applicationState.getId(),
          pipelineConfiguration);
        return null;
      }
    });
  }

  public ListenableFuture<ClusterPipelineStatus> getStatus(final ApplicationState applicationState,
                                             final PipelineConfiguration pipelineConfiguration)
                                             throws TimeoutException, IOException {
    return executorService.submit(new Callable<ClusterPipelineStatus>() {
      @Override
      public ClusterPipelineStatus call() throws Exception {
        return clusterProvider.getStatus(systemProcessFactory, clusterManagerFile, tempDir, applicationState.getId(),
          pipelineConfiguration);
      }
    });
  }

  private static String errorString(String template, Object... args) {
    return Utils.format("ERROR: " + template, args);
  }
}
