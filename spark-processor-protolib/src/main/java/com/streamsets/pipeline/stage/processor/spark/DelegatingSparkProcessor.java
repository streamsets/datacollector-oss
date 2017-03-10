/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.spark;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.stage.processor.spark.cluster.ClusterExecutorSparkProcessor;

import java.util.List;
import java.util.concurrent.Semaphore;

public class DelegatingSparkProcessor extends SingleLaneProcessor {

  private SingleLaneProcessor underlyingProcessor;
  private final SparkProcessorConfigBean conf;
  private final Semaphore initedSema;

  public DelegatingSparkProcessor(SparkProcessorConfigBean conf, Semaphore initedSema) {
    this.conf = conf;
    this.initedSema = initedSema;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (getContext().isPreview() || getContext().getExecutionMode() == ExecutionMode.STANDALONE) {
      underlyingProcessor = new StandaloneSparkProcessor(conf);
    } else {
      underlyingProcessor = new ClusterExecutorSparkProcessor();
    }
    issues.addAll(underlyingProcessor.init(getInfo(), getContext()));
    initedSema.release();
    return issues;
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
    underlyingProcessor.process(batch, singleLaneBatchMaker);
  }

  @VisibleForTesting
  Processor getUnderlyingProcessor() {
    return underlyingProcessor;
  }

  @Override
  public void destroy() {
    underlyingProcessor.destroy();
  }
}
