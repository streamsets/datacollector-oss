/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.cluster;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.SystemProcessFactory;

import java.io.File;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class MockSparkProvider implements SparkProvider {

  public boolean killTimesOut = false;
  public boolean submitTimesOut = false;
  public boolean isRunningTimesOut = false;
  public boolean isRunning = false;
  public String appId = null;

  @Override
  public void killPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir, String appId)
    throws TimeoutException {
    if (killTimesOut) {
      throw new TimeoutException();
    }
  }

  @Override
  public boolean isRunning(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir, String appId)
    throws TimeoutException {
    if (isRunningTimesOut) {
      throw new TimeoutException();
    }
    return isRunning;
  }

  @Override
  public String startPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                              Map<String, String> environment, Map<String, String> sourceInfo,
                              PipelineConfiguration pipelineConfiguration, StageLibraryTask stageLibrary,
                              File etcDir, File staticWebDir, File bootstrapDir, URLClassLoader apiCL,
                              URLClassLoader containerCL, int timeToWaitForFailure)
  throws TimeoutException {
    if (submitTimesOut) {
      throw new TimeoutException();
    }
    return appId;
  }
}
