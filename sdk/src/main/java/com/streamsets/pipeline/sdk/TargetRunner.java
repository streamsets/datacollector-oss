/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.runner.BatchImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TargetRunner extends StageRunner<Target> {

  @SuppressWarnings("unchecked")
  public TargetRunner(Target source, Map<String, Object> configuration, boolean isPreview) {
    super(source, configuration, Collections.EMPTY_LIST, isPreview);
  }

  @SuppressWarnings("unchecked")
  public TargetRunner(Class<Target> sourceClass, Map<String, Object> configuration, boolean isPreview) {
    super(sourceClass, configuration, Collections.EMPTY_LIST, isPreview);
  }

  public void runWrite(List<Record> inputRecords) throws StageException {
    ensureStatus(Status.INITIALIZED);
    BatchImpl batch = new BatchImpl(getInfo().getInstanceName(), "sdk:sourceOffset", inputRecords);
    getStage().write(batch);
  }

  public static class Builder extends StageRunner.Builder<Target, TargetRunner, Builder> {

    public Builder(Target processor) {
      super(processor);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Target> processorClass) {
      super((Class<Target>) processorClass);
    }

    @Override
    public TargetRunner build() {
      Utils.checkState(outputLanes.isEmpty(), "A Target cannot have output streams");
      return (stage != null) ? new TargetRunner(stage, configs, isPreview)
                             : new TargetRunner(stageClass, configs, isPreview);
    }

  }

}