/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.cluster;

import com.streamsets.datacollector.cluster.ApplicationState;
import com.streamsets.datacollector.cluster.ClusterPipelineStatus;
import com.streamsets.datacollector.cluster.ClusterProvider;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.SystemProcessFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class MockClusterProvider implements ClusterProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MockClusterProvider.class);
  public boolean killTimesOut = false;
  public boolean submitTimesOut = false;
  public boolean isRunningTimesOut = false;
  public boolean isRunningCommandFails = false;
  public boolean isSucceeded = false;
  public boolean isRunning = true;
  public String appId = null;

  @Override
  public void killPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir, String appId,
                           PipelineConfiguration pipelineConfiguration)
    throws TimeoutException {
    LOG.info("killPipeline");
    if (killTimesOut) {
      throw new TimeoutException();
    }
  }

  @Override
  public ClusterPipelineStatus getStatus(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir, String appId,
                           PipelineConfiguration pipelineConfiguration) throws TimeoutException {
    LOG.info("isRunning");
    if (isRunningTimesOut) {
      throw new TimeoutException();
    }
    if (isRunningCommandFails) {
      throw new RuntimeException("Mocking failure of isRunning");
    }
    if (isSucceeded) {
      return ClusterPipelineStatus.SUCCEEDED;
    }
    if (isRunning){
     return ClusterPipelineStatus.RUNNING;
    } else {
      return ClusterPipelineStatus.FAILED;
    }
  }

  @Override
  public ApplicationState startPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                              Map<String, String> environment, Map<String, String> sourceInfo,
                              PipelineConfiguration pipelineConfiguration, StageLibraryTask stageLibrary,
                              File etcDir, File resourcesDir, File staticWebDir, File bootstrapDir, URLClassLoader apiCL,
                              URLClassLoader containerCL, long timeout)
  throws TimeoutException {
    LOG.info("startPipeline");
    if (submitTimesOut) {
      throw new TimeoutException();
    }
    ApplicationState applicationState = new ApplicationState();
    applicationState.setId(appId);
    return applicationState;
  }
}
