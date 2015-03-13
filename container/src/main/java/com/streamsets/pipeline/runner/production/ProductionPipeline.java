/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;

import java.util.List;

public class ProductionPipeline {
  private final Pipeline pipeline;
  private final ProductionPipelineRunner pipelineRunner;

  public ProductionPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
    this.pipelineRunner =  (ProductionPipelineRunner)pipeline.getRunner();
  }

  public void run() throws StageException, PipelineRuntimeException{
    pipeline.init();
    try {
      pipeline.run();
    } finally {
      pipeline.destroy();
    }
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  public void stop() {
    pipelineRunner.stop();
  }

  public boolean wasStopped() {
    return pipelineRunner.wasStopped();
  }

  public String getCommittedOffset() {
    return pipelineRunner.getCommittedOffset();
  }

  public void captureSnapshot(int batchSize) {
    pipelineRunner.captureNextBatch(batchSize);
  }

  public void setOffset(String offset) {
    ProductionSourceOffsetTracker offsetTracker = (ProductionSourceOffsetTracker) pipelineRunner.getOffSetTracker();
    offsetTracker.setOffset(offset);
    offsetTracker.commitOffset();
  }

  public List<Record> getErrorRecords(String instanceName, int size) {
    return pipelineRunner.getErrorRecords(instanceName, size);
  }

  public List<ErrorMessage> getErrorMessages(String instanceName, int size) {
    return pipelineRunner.getErrorMessages(instanceName, size);
  }

  public long getLastBatchTime() {
    return pipelineRunner.getOffSetTracker().getLastBatchTime();
  }

}
