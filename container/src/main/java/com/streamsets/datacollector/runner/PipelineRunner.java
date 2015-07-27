/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.runner;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public interface PipelineRunner {

  public RuntimeInfo getRuntimeInfo();

  public boolean isPreview();

  public MetricRegistry getMetrics();

  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler) throws StageException, PipelineRuntimeException;

  public void run(Pipe[] pipes, BadRecordsHandler badRecordsHandler, List<StageOutput> stageOutputsToOverride)
      throws StageException, PipelineRuntimeException;

  public List<List<StageOutput>> getBatchesOutput();

  public String getSourceOffset();

  public String getNewSourceOffset();

  public void setObserver(Observer observer);

  public void registerListener(BatchListener batchListener);

}
