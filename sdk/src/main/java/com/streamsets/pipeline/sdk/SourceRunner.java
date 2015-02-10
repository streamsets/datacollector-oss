/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageType;

import java.util.List;
import java.util.Map;

public class SourceRunner extends StageRunner<Source> {

  public SourceRunner(Source source, Map<String, Object> configuration, List<String> outputLanes, boolean isPreview) {
    super(source, StageType.SOURCE, configuration, outputLanes, isPreview);
  }

  public SourceRunner(Class<Source> sourceClass, Map<String, Object> configuration, List<String> outputLanes,
      boolean isPreview) {
    super(sourceClass, StageType.SOURCE, configuration, outputLanes, isPreview);
  }

  public Output runProduce(String lastOffset, int maxBatchSize) throws StageException {
    ensureStatus(Status.INITIALIZED);
    BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context)getContext()).getOutputLanes());
    String newOffset = getStage().produce(lastOffset, maxBatchSize, batchMaker);
    return new Output(newOffset, batchMaker.getOutput());
  }

  public static class Builder extends StageRunner.Builder<Source, SourceRunner, Builder> {

    public Builder(Source source) {
      super(source);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Source> sourceClass) {
      super((Class<Source>) sourceClass);
    }

    @Override
    public SourceRunner build() {
      Utils.checkState(!outputLanes.isEmpty(), "A Source must have at least one output stream");
      return  (stage != null) ? new SourceRunner(stage, configs, outputLanes, isPreview)
                              : new SourceRunner(stageClass, configs, outputLanes, isPreview);
    }

  }

  public static BatchMaker createTestBatchMaker(String... outputLanes) {
    return StageRunner.createTestBatchMaker(outputLanes);
  }

  public static Output getOutput(BatchMaker batchMaker) {
    return StageRunner.getOutput(batchMaker);
  }

}
