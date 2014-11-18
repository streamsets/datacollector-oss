/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.sdk.testharness;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.testharness.internal.StageInfo;
import com.streamsets.pipeline.sdk.testharness.internal.BatchBuilder;
import com.streamsets.pipeline.sdk.testharness.internal.StageBuilder;
import com.streamsets.pipeline.sdk.testharness.internal.TargetContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    try {
      target.init(info, (Target.Context) context);
    } catch (StageException e) {
      LOG.error("Failed to init Processor. Message : " + e.getMessage());
      throw e;
    }
    try {
      target.write(batchBuilder.build());
    } catch (StageException e) {
      LOG.error("Failed to process. Message : " + e.getMessage());
      throw e;
    }
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
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
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
      info = new StageInfo(
        stageDefAnnot.name(), stageDefAnnot.version(), instanceName);
      //mockInfoAndContextForStage and stub Source.Context
      context = new TargetContextImpl(instanceName);

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
