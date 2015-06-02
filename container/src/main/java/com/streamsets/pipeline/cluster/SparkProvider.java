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

public interface SparkProvider {

  void killPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                   String appId, PipelineConfiguration pipelineConfiguration) throws TimeoutException;

  ClusterPipelineStatus getStatus(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                   String appId, PipelineConfiguration pipelineConfiguration) throws TimeoutException;


  ApplicationState startPipeline(SystemProcessFactory systemProcessFactory, File sparkManager, File tempDir,
                                      Map<String, String> environment, Map<String, String> sourceInfo,
                                      PipelineConfiguration pipelineConfiguration, StageLibraryTask stageLibrary,
                                      File etcDir, File resourcesDir, File staticWebDir, File bootstrapDir,
                                      URLClassLoader apiCL, URLClassLoader containerCL, long timeToWaitForFailure)
    throws TimeoutException;
}
