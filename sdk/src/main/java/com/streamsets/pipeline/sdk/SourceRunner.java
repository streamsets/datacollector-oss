/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.container.Utils;

import java.util.Map;
import java.util.Set;

public class SourceRunner extends StageRunner<Source> {

  public SourceRunner(Source source, Map<String, Object> configuration, Set<String> outputLanes) {
    super(source, configuration, outputLanes);
  }

  public SourceRunner(Class<Source> sourceClass, Map<String, Object> configuration, Set<String> outputLanes) {
    super(sourceClass, configuration, outputLanes);
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
      Utils.checkState(!outputLanes.isEmpty(), "A Source must have at least one output lane");
      return  (stage != null) ? new SourceRunner(stage, configs, outputLanes)
                              : new SourceRunner(stageClass, configs, outputLanes);
    }

  }

}
