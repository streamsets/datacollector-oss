/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.SystemProcessFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class MockSparkProvider implements SparkProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MockSparkProvider.class);
  public boolean killTimesOut = false;
  public boolean submitTimesOut = false;
  public boolean isRunningTimesOut = false;
  public boolean isRunningCommandFails = false;
  public boolean isRunning = false;
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
  public boolean isRunning(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir, String appId,
                           PipelineConfiguration pipelineConfiguration) throws TimeoutException {
    LOG.info("isRunning");
    if (isRunningTimesOut) {
      throw new TimeoutException();
    }
    if (isRunningCommandFails) {
      throw new RuntimeException("Mocking failure of isRunning");
    }
    return isRunning;
  }

  @Override
  public ApplicationState startPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                              Map<String, String> environment, Map<String, String> sourceInfo,
                              PipelineConfiguration pipelineConfiguration, StageLibraryTask stageLibrary,
                              File etcDir, File staticWebDir, File bootstrapDir, URLClassLoader apiCL,
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
