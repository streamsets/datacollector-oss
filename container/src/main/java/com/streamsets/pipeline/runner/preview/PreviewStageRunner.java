/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.Observer;
import com.streamsets.pipeline.runner.Pipe;
import com.streamsets.pipeline.runner.PipeBatch;
import com.streamsets.pipeline.runner.PipelineRunner;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.runner.StagePipe;
import com.streamsets.pipeline.util.ContainerError;

import java.util.ArrayList;
import java.util.List;

public class PreviewStageRunner implements PipelineRunner {
  private final String instanceName;
  private List<Record> inputRecords;
  private final MetricRegistry metrics;
  private final List<List<StageOutput>> batchesOutput;

  public PreviewStageRunner(String instanceName, List<Record> inputRecords) {
    this.instanceName = instanceName;
    this.inputRecords = inputRecords;
    this.metrics = new MetricRegistry();
    this.batchesOutput = new ArrayList<>();
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(Pipe[] pipes) throws StageException, PipelineRuntimeException {
    Pipe stagePipe =  null;
    for (Pipe pipe : pipes) {
      if (pipe.getStage().getInfo().getInstanceName().equals(instanceName) && pipe instanceof StagePipe) {
        stagePipe = pipe;
        break;
      }
    }
    if (stagePipe != null) {
      if (stagePipe.getStage().getDefinition().getType() == StageType.SOURCE) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0157, instanceName);
      }
      PipeBatch pipeBatch = new StagePreviewPipeBatch(instanceName, inputRecords);
      stagePipe.process(pipeBatch);
      batchesOutput.add(pipeBatch.getSnapshotsOfAllStagesOutput());

    } else {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0156, instanceName);
    }
  }

  @Override
  public void run(Pipe[] pipes, List<StageOutput> stageOutputsToOverride)
      throws StageException, PipelineRuntimeException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSourceOffset() {
    return null;
  }

  @Override
  public String getNewSourceOffset() {
    return null;
  }

  @Override
  public void setObserver(Observer observer) {

  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    return batchesOutput;
  }

}
