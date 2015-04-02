/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.BatchImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TargetRunner extends StageRunner<Target> {

  @SuppressWarnings("unchecked")
  public TargetRunner(Class<Target> targetClass, Target target, Map<String, Object> configuration, boolean isPreview,
                      OnRecordError onRecordError, Map<String, Object> constants) {
    super(targetClass, target, StageType.TARGET, configuration, Collections.EMPTY_LIST, isPreview, onRecordError,
      constants);
  }

  @SuppressWarnings("unchecked")
  public TargetRunner(Class<Target> sourceClass, Map<String, Object> configuration, boolean isPreview,
      OnRecordError onRecordError, Map<String, Object> constants) {
    super(sourceClass, StageType.TARGET, configuration, Collections.EMPTY_LIST, isPreview, onRecordError, constants);
  }

  public void runWrite(List<Record> inputRecords) throws StageException {
    ensureStatus(Status.INITIALIZED);
    BatchImpl batch = new BatchImpl(getInfo().getInstanceName(), "sdk:sourceOffset", inputRecords);
    getStage().write(batch);
  }

  public static class Builder extends StageRunner.Builder<Target, TargetRunner, Builder> {

    public Builder(Class<? extends Target> targetClass, Target target) {
      super((Class<Target>)targetClass, target);
    }

    @SuppressWarnings("unchecked")
    public Builder(Class<? extends Target> targetClass) {
      super((Class<Target>) targetClass);
    }

    @Override
    public TargetRunner build() {
      Utils.checkState(outputLanes.isEmpty(), "A Target cannot have output streams");
      return (stage != null) ? new TargetRunner(stageClass, stage, configs, isPreview, onRecordError, constants)
                             : new TargetRunner(stageClass, configs, isPreview, onRecordError, constants);
    }

  }

}