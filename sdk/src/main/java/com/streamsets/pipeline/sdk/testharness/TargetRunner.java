/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.testharness;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.runner.StageContext;
import com.streamsets.pipeline.sdk.testharness.internal.StageInfo;
import com.streamsets.pipeline.sdk.testharness.internal.BatchBuilder;
import com.streamsets.pipeline.sdk.testharness.internal.StageBuilder;
import com.streamsets.pipeline.sdk.util.StageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class TargetRunner <T extends Target> {

  private static final Logger LOG = LoggerFactory.getLogger(TargetRunner.class);

  private final T target;
  private final BatchBuilder batchBuilder;
  private final Stage.Info info;
  private final Stage.Context context;

  /*******************************************************/
  /***************** public methods **********************/
  /*******************************************************/

  public void run() throws StageException {
    init();
    write();
    destroy();
  }

  public void init() throws StageException {
    try {
      target.init(info, (Target.Context) context);
    } catch (StageException e) {
      LOG.error("Failed to init Processor. Message : " + e.getMessage());
      throw e;
    }
  }

  public void write() throws StageException {
    try {
      target.write(batchBuilder.build());
    } catch (StageException e) {
      LOG.error("Failed to process. Message : " + e.getMessage());
      throw e;
    }
  }

  public void destroy() {
    target.destroy();
  }

  /*******************************************************/
  /***************** Builder class ***********************/
  /*******************************************************/

  public static class Builder<T extends Target> extends StageBuilder {

    private final BatchBuilder batchBuilder;

    public Builder(RecordProducer recordProducer) {
      this.batchBuilder = new BatchBuilder(recordProducer);
    }

    public Builder<T> sourceOffset(String sourceOffset) {
      this.sourceOffset = sourceOffset;
      return this;
    }

    public Builder<T> maxBatchSize(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public Builder<T> addTarget(T target) {
      this.stage = target;
      return this;
    }

    public Builder<T> addTarget(Class<T> klass) {
      try {
        this.stage = klass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        e.printStackTrace();
      }
      return this;
    }

    public Builder<T> configure(String key, Object value) {
      configMap.put(key, value);
      return this;
    }

    public TargetRunner<T> build() throws StageException {

      //validate that all required options are set
      if(!validateTarget()) {
        throw new IllegalStateException(
          "TargetBuilder is not configured correctly. Please check the logs for errors.");
      }

      //configure the stage
      configureStage();

      //extract name and version of the stage from the stage def annotation
      StageDef stageDefAnnot = stage.getClass().getAnnotation(StageDef.class);
      info = new StageInfo(StageHelper.getStageNameFromClassName(stage.getClass().getName()), stageDefAnnot.version(),
          instanceName);
      //mockInfoAndContextForStage and stub Source.Context
      context = new StageContext(instanceName, (Set) ImmutableSet.of());

      //update batchbuilder
      batchBuilder.setSourceOffset(sourceOffset);
      batchBuilder.setMaxBatchSize(maxBatchSize);

      return new TargetRunner((T)stage, batchBuilder, info, context);
    }

    private boolean validateTarget() {
      return validateStage();
    }
  }

  /*******************************************************/
  /***************** private methods *********************/
  /*******************************************************/

  private TargetRunner(T target, BatchBuilder batchBuilder,
                       Stage.Info info, Stage.Context context) {
    this.target = target;
    this.batchBuilder = batchBuilder;
    this.info = info;
    this.context = context;
  }


}
