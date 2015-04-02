/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.BatchImpl;

import java.util.List;
import java.util.Map;

public class ProcessorRunner extends StageRunner<Processor> {

  public ProcessorRunner(Class<Processor> processorClass, Processor processor, Map<String, Object> configuration, List<String> outputLanes,
      boolean isPreview, OnRecordError onRecordError, Map<String, Object> constants) {
    super(processorClass, processor, StageType.PROCESSOR, configuration, outputLanes, isPreview, onRecordError, constants);
  }

  public ProcessorRunner(Class<Processor> processorClass, Map<String, Object> configuration, List<String> outputLanes,
      boolean isPreview, OnRecordError onRecordError, Map<String, Object> constants) {
    super(processorClass, StageType.PROCESSOR, configuration, outputLanes, isPreview, onRecordError, constants);
  }

  public Output runProcess(List<Record> inputRecords) throws StageException {
    ensureStatus(Status.INITIALIZED);
    BatchImpl batch = new BatchImpl(getInfo().getInstanceName(), "sdk:sourceOffset", inputRecords);
    BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context)getContext()).getOutputLanes());
    getStage().process(batch, batchMaker);
    return new Output("sdk:sourceOffset", batchMaker.getOutput());
  }

  public static class Builder extends StageRunner.Builder<Processor, ProcessorRunner, Builder> {

    public Builder(Class<? extends Processor> processorClass,  Processor processor) {
      super((Class<Processor>)processorClass, processor);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Processor> processorClass) {
      super((Class<Processor>) processorClass);
    }

    @Override
    public ProcessorRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Processor must have at least one output stream");
      return  (stage != null) ? new ProcessorRunner(stageClass, stage, configs, outputLanes, isPreview, onRecordError, constants)
                              : new ProcessorRunner(stageClass, configs, outputLanes, isPreview, onRecordError, constants);
    }

  }

  public static BatchMaker createTestBatchMaker(String... outputLanes) {
    return StageRunner.createTestBatchMaker(outputLanes);
  }

  public static Output getOutput(BatchMaker batchMaker) {
    return StageRunner.getOutput(batchMaker);
  }

}
