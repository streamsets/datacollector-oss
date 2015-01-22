/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public interface PipelineRunner {

  public MetricRegistry getMetrics();

  public void run(Pipe[] pipes)  throws StageException, PipelineRuntimeException;

  public List<List<StageOutput>> getBatchesOutput();

  public String getSourceOffset();

  public String getNewSourceOffset();

  public void setObserver(Observer observer);

}
