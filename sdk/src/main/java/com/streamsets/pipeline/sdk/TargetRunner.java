/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TargetRunner extends StageRunner<Target> {
  private static final Logger LOG = LoggerFactory.getLogger(TargetRunner.class);

  @SuppressWarnings("unchecked")
  public TargetRunner(Class<Target> targetClass, Target target, Map<String, Object> configuration, boolean isPreview,
                      OnRecordError onRecordError, Map<String, Object> constants, boolean isClusterMode,
      String resourcesDir) {
    super(targetClass, target, StageType.TARGET, configuration, Collections.EMPTY_LIST, isPreview, onRecordError,
      constants, isClusterMode, resourcesDir);
  }

  @SuppressWarnings("unchecked")
  public TargetRunner(Class<Target> sourceClass, Map<String, Object> configuration, boolean isPreview,
      OnRecordError onRecordError, Map<String, Object> constants, boolean isClusterMode, String resourcesDir) {
    super(sourceClass, StageType.TARGET, configuration, Collections.EMPTY_LIST, isPreview, onRecordError, constants,
      isClusterMode, resourcesDir);
  }

  public void runWrite(List<Record> inputRecords) throws StageException {
    LOG.debug("Stage '{}' write starts", getInfo().getInstanceName());
    ensureStatus(Status.INITIALIZED);
    BatchImpl batch = new BatchImpl(getInfo().getInstanceName(), "sdk:sourceOffset", inputRecords);
    getStage().write(batch);
    LOG.debug("Stage '{}' write ends", getInfo().getInstanceName());
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
      return (stage != null) ?
        new TargetRunner(stageClass, stage, configs, isPreview, onRecordError, constants, isClusterMode, resourcesDir)
        : new TargetRunner(stageClass, configs, isPreview, onRecordError, constants, isClusterMode, resourcesDir);
    }

  }

}