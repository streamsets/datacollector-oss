/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;

/**
 * BatchContext implementation keeping all the state for given Batch while the pipeline is running the origin's code.
 */
public class BatchContextImpl implements BatchContext {

  /**
   * Pipe batch associated with this batch.
   */
  private FullPipeBatch pipeBatch;

  /**
   * Batch maker associated with the origin's stage.
   */
  private BatchMaker batchMaker;

  /**
   * Start time of the batch execution.
   */
  private long startTime;

  public BatchContextImpl(FullPipeBatch pipeBatch) {
    this.pipeBatch = pipeBatch;
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public BatchMaker getBatchMaker() {
    return batchMaker;
  }

  public void setBatchMaker(BatchMaker batchMaker) {
    this.batchMaker = batchMaker;
  }

  public FullPipeBatch getPipeBatch() {
    return pipeBatch;
  }

  public long getStartTime() {
    return startTime;
  }
}
