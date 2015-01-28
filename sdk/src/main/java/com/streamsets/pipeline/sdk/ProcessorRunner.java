/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.runner.BatchImpl;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorRunner extends StageRunner<Processor> {

  public ProcessorRunner(Processor processor, Map<String, Object> configuration, Set<String> outputLanes) {
    super(processor, configuration, outputLanes);
  }

  public ProcessorRunner(Class<Processor> processorClass, Map<String, Object> configuration, Set<String> outputLanes) {
    super(processorClass, configuration, outputLanes);
  }

  public Output runProcess(List<Record> inputRecords) throws StageException {
    ensureStatus(Status.INITIALIZED);
    BatchImpl batch = new BatchImpl(getInfo().getInstanceName(), "sdk:sourceOffset", inputRecords);
    BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context)getContext()).getOutputLanes());
    getStage().process(batch, batchMaker);
    return new Output("sdk:sourceOffset", batchMaker.getOutput());
  }

  public static class Builder extends StageRunner.Builder<Processor, ProcessorRunner, Builder> {

    public Builder(Processor processor) {
      super(processor);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Processor> processorClass) {
      super((Class<Processor>) processorClass);
    }

    @Override
    public ProcessorRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Processor must have at least one output stream");
      return  (stage != null) ? new ProcessorRunner(stage, configs, outputLanes)
                              : new ProcessorRunner(stageClass, configs, outputLanes);
    }

  }

  public static BatchMaker createTestBatchMaker(String... outputLanes) {
    return StageRunner.createTestBatchMaker(outputLanes);
  }

  public static Output getOutput(BatchMaker batchMaker) {
    return StageRunner.getOutput(batchMaker);
  }

}
