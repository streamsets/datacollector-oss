/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageType;

import java.util.List;
import java.util.Map;

public class SourceRunner extends StageRunner<Source> {

  public SourceRunner(Class<Source> sourceClass, Source source, Map<String, Object> configuration, List<String> outputLanes, boolean isPreview,
      OnRecordError onRecordError) {
    super(sourceClass, source, StageType.SOURCE, configuration, outputLanes, isPreview, onRecordError);
  }

  public SourceRunner(Class<Source> sourceClass, Map<String, Object> configuration, List<String> outputLanes,
      boolean isPreview, OnRecordError onRecordError) {
    super(sourceClass, StageType.SOURCE, configuration, outputLanes, isPreview, onRecordError);
  }

  public Output runProduce(String lastOffset, int maxBatchSize) throws StageException {
    ensureStatus(Status.INITIALIZED);
    BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context)getContext()).getOutputLanes());
    String newOffset = getStage().produce(lastOffset, maxBatchSize, batchMaker);
    return new Output(newOffset, batchMaker.getOutput());
  }

  public static class Builder extends StageRunner.Builder<Source, SourceRunner, Builder> {

    public Builder(Class<? extends Source> sourceClass,  Source source) {
      super((Class<Source>)sourceClass, source);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Source> sourceClass) {
      super((Class<Source>) sourceClass);
    }

    @Override
    public SourceRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Source must have at least one output stream");
      return  (stage != null) ? new SourceRunner(stageClass, stage, configs, outputLanes, isPreview, onRecordError)
                              : new SourceRunner(stageClass, configs, outputLanes, isPreview, onRecordError);
    }

  }

  public static BatchMaker createTestBatchMaker(String... outputLanes) {
    return StageRunner.createTestBatchMaker(outputLanes);
  }

  public static Output getOutput(BatchMaker batchMaker) {
    return StageRunner.getOutput(batchMaker);
  }

}
